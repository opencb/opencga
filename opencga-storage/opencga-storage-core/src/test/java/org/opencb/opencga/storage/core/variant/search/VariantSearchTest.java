package org.opencb.opencga.storage.core.variant.search;

import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.Score;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageTest;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadingWatchdog;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class VariantSearchTest extends VariantStorageBaseTest implements DummyVariantStorageTest {

    @Rule
    public VariantSolrExternalResource solr = new VariantSolrExternalResource(this);

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        solr.printCollections(Paths.get(newOutputUri("searchIndex_" + TimeUtils.getTime() + "_solr")));
    }

    @Test
    public void testTranscriptInfo() throws Exception {
        int limit = 500;

        solr.configure(variantStorageEngine);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();

        System.out.println(smallInputUri.getPath());

        StudyMetadata studyMetadata = metadataManager.createStudy("s1");

        List<Variant> variants = getVariants(limit);
        List<Variant> annotatedVariants = annotatedVariants(variants, studyMetadata.getName());


        SearchIndexMetadata indexMetadata = variantSearchManager.createIndexMetadataIfEmpty();
        variantSearchManager.insert(indexMetadata, annotatedVariants);

        VariantQueryResult<Variant> results = variantSearchManager.query(indexMetadata, variantStorageEngine.parseQuery(new Query(),
                new QueryOptions(QueryOptions.LIMIT, limit)));

        for (int i = 0; i < limit; i++) {
            Variant expectedVariant = annotatedVariants.get(i);
            Variant actualVariant = results.getResults().get(i);

            assertEquals(expectedVariant.toString(), actualVariant.toString());
        }

        System.out.println("#variants = " + variants.size());
        System.out.println("#annotations = " + annotatedVariants.size());
        System.out.println("#variants from Solr = " + results.getResults().size());
    }

    @Test
    public void testLargeVariantsV1() throws Exception {
        testLargeVariants("v1");
    }

    @Test
    public void testLargeVariantsV2() throws Exception {
        testLargeVariants("v2");
    }

    public void testLargeVariants(String idGenVer) throws Exception {
        String variantId = "1:1000:-:" + RandomStringUtils.random(500000, 'A', 'C', 'G', 'T');
        Variant variant = new Variant(variantId);

        variantStorageEngine.getOptions().put(VariantStorageOptions.SEARCH_STATS_VARIANT_ID_VERSION.key(), idGenVer);
        solr.configure(variantStorageEngine);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();


        SearchIndexMetadata indexMetadata = variantSearchManager.createIndexMetadataIfEmpty();
        assertEquals(idGenVer, indexMetadata.getAttributes().getString(VariantStorageOptions.SEARCH_STATS_VARIANT_ID_VERSION.key(), null));
        variantSearchManager.insert(indexMetadata, Collections.singletonList(variant));

        VariantQueryResult<Variant> results = variantSearchManager.query(indexMetadata, variantStorageEngine.parseQuery(new Query(),
                new QueryOptions()));

        assertEquals(1, results.getNumMatches());
        Variant readVariant = results.first();
        assertEquals(variantId, readVariant.toString());
    }

    @Test
    public void testSpecialCharacter() throws Exception {
        int limit = 1;

        VariantStorageMetadataManager scm = variantStorageEngine.getMetadataManager();

        solr.configure(variantStorageEngine);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();

        System.out.println(smallInputUri.getPath());

        List<Variant> variants = getVariants(limit);
        List<Variant> annotatedVariants = annotatedVariants(variants);

        String study = "abyu12";
        String file = "a.vcf";

        variants.get(0).getStudies().get(0).getFiles().get(0).setFileId(file);
        System.out.println(variants.get(0).getStudies().get(0).getFiles().get(0).getFileId());

        int studyId = scm.createStudy(study).getId();
        int fileId = scm.registerFile(studyId, file, Arrays.asList("A-A", "B", "C", "D"));
        scm.addIndexedFiles(studyId, Collections.singletonList(fileId));
        SearchIndexMetadata indexMetadata = variantSearchManager.createIndexMetadataIfEmpty();
        variantSearchManager.insert(indexMetadata, annotatedVariants);

        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), study);
//        query.put(VariantQueryParam.SAMPLE.key(), samplePosition.keySet().toArray()[0]);
        query.put(VariantQueryParam.FILE.key(), file);
        query.put(VariantQueryParam.FILTER.key(), "PASS");
        query.put(VariantQueryParam.ANNOT_CLINICAL_SIGNIFICANCE.key(), "benign");
        VariantQueryResult<Variant> results = variantSearchManager.query(indexMetadata, variantStorageEngine.parseQuery(query,
                new QueryOptions(QueryOptions.LIMIT, limit)));

        if (results.getResults().size() > 0) {
            System.out.println(results.getResults().get(0).toJson());
        } else {
            System.out.println("Not found!!!!");
        }
    }

    @Test
    public void testWhileLoadingEvent() throws Exception {
        solr.configure(variantStorageEngine);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();

        List<Variant> variants = annotatedVariants(getVariants(10));

        SearchIndexMetadata indexMetadata = variantSearchManager.createIndexMetadataIfEmpty();
        variantSearchManager.insert(indexMetadata, variants);

        VariantQueryResult<Variant> result = variantStorageEngine.get(new Query(), new QueryOptions(VariantSearchManager.USE_SEARCH_INDEX, "YES"));
        assertTrue(result.getSource().contains("solr"));
        assertTrue(result.getEvents().isEmpty());

        VariantSearchLoadingWatchdog watchdog = new VariantSearchLoadingWatchdog(variantStorageEngine.getMetadataManager(), 1, TimeUnit.MINUTES);
        watchdog.start();

        result = variantStorageEngine.get(new Query(), new QueryOptions(VariantSearchManager.USE_SEARCH_INDEX, "YES"));
        assertTrue(result.getSource().contains("solr"));
        assertFalse(result.getEvents().isEmpty());

        watchdog.stopWatchdog();

        result = variantStorageEngine.get(new Query(), new QueryOptions(VariantSearchManager.USE_SEARCH_INDEX, "YES"));
        assertTrue(result.getSource().contains("solr"));
        assertTrue(result.getEvents().isEmpty());

    }

    @Test
    public void testTypeFacet() throws Exception {
        int limit = 500;

        solr.configure(variantStorageEngine);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();

        System.out.println(smallInputUri.getPath());

        List<Variant> variants = getVariants(limit);
        List<Variant> annotatedVariants = annotatedVariants(variants);

        metadataManager.createStudy("s1");

        SearchIndexMetadata indexMetadata = variantSearchManager.createIndexMetadataIfEmpty();
        variantSearchManager.insert(indexMetadata, annotatedVariants);

        QueryOptions queryOptions = new QueryOptions();
        String facet = "type[INDEL,SNV]";
        queryOptions.put(QueryOptions.FACET, facet);
        DataResult<FacetField> facetQueryResult = variantSearchManager.facetedQuery(indexMetadata, new Query(), queryOptions);
        String s = JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facetQueryResult);
        System.out.println(s);

        FacetField facetField = facetQueryResult.first();
        Assert.assertEquals(499, facetField.getCount());
        Assert.assertEquals(2, facetField.getBuckets().size());
        Assert.assertTrue(facetField.getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList()).contains("SNV"));
        Assert.assertTrue(facetField.getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList()).contains("INDEL"));
        for (FacetField.Bucket bucket : facetField.getBuckets()) {
            if (bucket.getValue().equals("SNV")) {
                Assert.assertEquals(490, bucket.getCount());
            } else if (bucket.getValue().equals("INDEL")) {
                Assert.assertEquals(9, bucket.getCount());
            }
        }
    }

    @Test
    public void testGeneFacet() throws Exception {
        int limit = 500;

        VariantStorageMetadataManager scm = variantStorageEngine.getMetadataManager();

        solr.configure(variantStorageEngine);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();

        System.out.println(smallInputUri.getPath());

        List<Variant> variants = getVariants(limit);
        List<Variant> annotatedVariants = annotatedVariants(variants);

        metadataManager.createStudy("s1");

        SearchIndexMetadata indexMetadata = variantSearchManager.createIndexMetadataIfEmpty();
        variantSearchManager.insert(indexMetadata, annotatedVariants);

        QueryOptions queryOptions = new QueryOptions();
        //String facet = "type[SNV,TOTO]>>biotypes";
        String facet = "genes[CDK11A,WDR78,ENSG00000115183,TOTO]>>type[INDEL,DELETION,SNV]";
        queryOptions.put(QueryOptions.FACET, facet);
        DataResult<FacetField> facetQueryResult = variantSearchManager.facetedQuery(indexMetadata, new Query(), queryOptions);
        String s = JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facetQueryResult);
        System.out.println(s);
        //        System.out.println(facetQueryResult.toString());
    }

    public void regex() throws Exception {
        String facet = "genes[G1,G2]>>type[INDEL,SNV];aggr(genes);biotypes";
        Map<String, Set<String>> includeMap = new FacetQueryParser().getIncludingValuesMap(facet);

        System.out.println(facet);
        if (MapUtils.isNotEmpty(includeMap)) {
            for (String key : includeMap.keySet()) {
                System.out.println("key: " + key);
                if (includeMap.containsKey(key) && CollectionUtils.isNotEmpty(includeMap.get(key))) {
                    for (String value : includeMap.get(key)) {
                        System.out.println("\t" + value);
                    }
                }

            }
        }
    }



    private Map<String, ConsequenceType> getConsequenceTypeMap(Variant variant){
        Map<String, ConsequenceType> map = new HashMap<>();
        if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
            for (ConsequenceType consequenceType: variant.getAnnotation().getConsequenceTypes()) {
                if (StringUtils.isNotEmpty(consequenceType.getTranscriptId())) {
                    map.put(consequenceType.getTranscriptId(), consequenceType);
                }
            }
        }
        return map;
    }

    private Score getScore(List<Score> scores, String source) {
        if (ListUtils.isNotEmpty(scores) && org.apache.commons.lang3.StringUtils.isNotEmpty(source)) {
            for (Score score: scores) {
                if (source.equals(score.getSource())) {
                    return score;
                }
            }
        }
        return null;
    }

    private void checkScore(List<Score> inScores, List<Score> outScores, String source) {
        Score inScore = getScore(inScores, source);
        Score outScore = getScore(outScores, source);

        if (inScore != null && outScore != null) {
            double inValue = inScore.getScore();
            double outValue = outScore.getScore();
            String inDescription = inScore.getDescription() == null ? "" : inScore.getDescription();
            String outDescription = outScore.getDescription() == null ? "" : outScore.getDescription();
            System.out.println(source + ": " + inValue + " vs " + outValue
                    + " ; " + inDescription + " vs " + outDescription);
        } else if (inScore != null || outScore != null) {
            fail("Mismatchtch " + source + " values");
        }
    }

    private List<Variant> getVariants(int limit) throws Exception {
        VariantVcfHtsjdkReader reader = variantReaderUtils.getVariantVcfReader(Paths.get(smallInputUri.getPath()), null);
        reader.open();
        reader.pre();
        VCFHeader vcfHeader = reader.getVCFHeader();
        List<Variant> variants = reader.read(limit);

        reader.post();
        reader.close();
        return variants;
    }

    private List<Variant> annotatedVariants(List<Variant> variants) throws IOException {
        return annotatedVariants(variants, "");
    }

    private List<Variant> annotatedVariants(List<Variant> variants, String studyId) throws IOException {
        CellBaseClient cellBaseClient = new CellBaseClient(variantStorageEngine.getConfiguration().getCellbase().toClientConfiguration());
        CellBaseDataResponse<VariantAnnotation> queryResponse = cellBaseClient.getVariantClient().getAnnotationByVariantIds(variants.stream().map(Variant::toString).collect(Collectors.toList()), QueryOptions.empty());

        // Set annotations
        for (int i = 0; i < variants.size(); i++) {
            variants.get(i).setAnnotation(queryResponse.getResponses().get(i).first());
            variants.get(i).getStudies().get(0).setStudyId(studyId);
        }
        return variants;
    }
}
