package org.opencb.opencga.storage.core.variant.query.executors;

import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;

@Ignore
public abstract class VariantQueryExecutorTest extends VariantStorageBaseTest {

    private static Logger logger = LoggerFactory.getLogger(VariantQueryExecutorTest.class);

    protected boolean fileIndexed;
    private StudyMetadata studyMetadata;
    private VariantFileMetadata fileMetadata;
    private int numVariants;
    private DBAdaptorVariantQueryExecutor dbQueryExecutor;
    private List<VariantQueryExecutor> variantQueryExecutors;

    @Before
    public void setUp() throws Exception {

        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        if (!fileIndexed) {
            studyMetadata = newStudyMetadata();
//            variantSource = new VariantSource(smallInputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");
            clearDB(DB_NAME);
            ObjectMap params = new ObjectMap()
                    .append(VariantStorageOptions.ASSEMBLY.key(), "GRCH38")
                    .append(VariantStorageOptions.ANNOTATE.key(), true)
                    .append(VariantStorageOptions.ANNOTATION_CHECKPOINT_SIZE.key(), 500)
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), true);

            StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyMetadata, params);
            fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(Paths.get(etlResult.getTransformResult().getPath()).toUri());
            numVariants = getExpectedNumLoadedVariants(fileMetadata);
            fileIndexed = true;
            Integer indexedFileId = metadataManager.getIndexedFiles(studyMetadata.getId()).iterator().next();

            //Calculate stats
            QueryOptions options = new QueryOptions(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                    .append(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100)
                    .append(DefaultVariantStatisticsManager.OUTPUT, outputUri)
                    .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, "cohort1.cohort2.stats");
            Iterator<Integer> iterator = metadataManager.getFileMetadata(studyMetadata.getId(), indexedFileId).getSamples().iterator();

            /** Create cohorts **/
            HashSet<String> cohort1 = new HashSet<>();
            cohort1.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));
            cohort1.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));

            HashSet<String> cohort2 = new HashSet<>();
            cohort2.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));
            cohort2.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));

            Map<String, Set<String>> cohorts = new HashMap<>();
            cohorts.put("cohort1", cohort1);
            cohorts.put("cohort2", cohort2);
            metadataManager.registerCohorts(studyMetadata.getName(), cohorts);

            variantStorageEngine.calculateStats(studyMetadata.getName(),
                    new ArrayList<>(cohorts.keySet()), options);


            variantQueryExecutors = variantStorageEngine.getVariantQueryExecutors();
            dbQueryExecutor = null;
            for (VariantQueryExecutor variantQueryExecutor : variantQueryExecutors) {
                if (variantQueryExecutor instanceof DBAdaptorVariantQueryExecutor) {
                    dbQueryExecutor = (DBAdaptorVariantQueryExecutor) variantQueryExecutor;
                    break;
                }
            }
            Assert.assertNotNull(dbQueryExecutor);
        }
    }

    @Test
    public void testXRefRs() throws StorageEngineException {
        Map<String, Matcher<Variant>> matchers = new LinkedHashMap<>();
        matchers.put("3:108634973:C:A", hasAnnotation(at("3:108634973:C:A")));
//        matchers.put("rs2032582", with("id", Variant::getId, is("rs2032582")));
        matchers.put("HP:0001250", hasAnnotation(with("GeneTraitAssociation", VariantAnnotation::getGeneTraitAssociation,
                hasItem(with("HPO", GeneTraitAssociation::getHpo, is("HP:0001250"))))));
        matchers.put("VAR_031174", hasAnnotation(with("ConsequenceType", VariantAnnotation::getConsequenceTypes,
                hasItem(with("ProteinVariantAnnotation", ConsequenceType::getProteinVariantAnnotation,
                        with("UniprotVariantId", ProteinVariantAnnotation::getUniprotVariantId, is("VAR_031174")))))));
//        matchers.put("Q9BY64", hasAnnotation(
//                with("ConsequenceType", VariantAnnotation::getConsequenceTypes, hasItem(
//                        with("ProteinVariantAnnotation", ConsequenceType::getProteinVariantAnnotation,
//                                with("UniprotAccession", ProteinVariantAnnotation::getUniprotAccession,
//                                        is("Q9BY64")))))));
        matchers.put("ENSG00000170925", hasAnnotation(
                with("ConsequenceType", VariantAnnotation::getConsequenceTypes, hasItem(
                        with("GeneId", ConsequenceType::getGeneId,
                                is("ENSG00000170925"))))));
        matchers.put("TEX13B", hasAnnotation(
                with("ConsequenceType", VariantAnnotation::getConsequenceTypes, hasItem(
                        with("GeneName", ConsequenceType::getGeneName,
                                is("TEX13B"))))));
        matchers.put("COSV60260399", hasAnnotation(with("TraitAssociation", VariantAnnotation::getTraitAssociation, hasItem(
                with("Cosmic", EvidenceEntry::getId, is("COSV60260399"))))));
        matchers.put("ENST00000341832.11(ENSG00000248333):c.356-1170A>G", hasAnnotation(with("HGVS", VariantAnnotation::getHgvs, hasItem(
                 is("ENST00000341832.11(ENSG00000248333):c.356-1170A>G")))));

        for (Map.Entry<String, Matcher<Variant>> entry : matchers.entrySet()) {
            testQuery(new VariantQuery().xref(entry.getKey()), new QueryOptions(), entry.getValue());
            VariantQueryResult<Variant> result = testQuery(new VariantQuery().xref(entry.getKey())
                    .study(studyMetadata.getName())
                    .includeSampleId(true)
                    .includeSampleAll(), new QueryOptions(), entry.getValue());
            if (result.getNumResults() == 1) {
                for (SampleEntry sample : result.first().getStudies().get(0).getSamples()) {
                    if (GenotypeClass.MAIN_ALT.test(sample.getData().get(0))) {
                        testQuery(new VariantQuery().xref(entry.getKey())
                                        .study(studyMetadata.getName())
                                        .includeSampleId(true)
                                        .sample(sample.getSampleId()),
                                new QueryOptions(), entry.getValue());
                    }
                }
            } else {
                List<String> samples = result.first().getStudies().get(0).getOrderedSamplesName();
                testQuery(new VariantQuery().xref(entry.getKey())
                                .study(studyMetadata.getName())
                                .includeSampleId(true)
                                .sample(samples),
                        new QueryOptions(), entry.getValue());
            }
//            Variant v = result.first();
//
//            for (SampleEntry sample : v.getStudies().get(0).getSamples()) {
//                if (GenotypeClass.MAIN_ALT.test(sample.getData().get(0))) {
//                    testQuery(new VariantQuery().xref(entry.getKey())
//                                    .study(studyMetadata.getName())
//                                    .includeSampleId(true)
//                                    .sample(sample.getSampleId()),
//                            new QueryOptions(), entry.getValue());
//                }
//            }
        }
    }

    public VariantQueryResult<Variant> testQuery(Query query, QueryOptions options, Matcher<Variant> matcher) throws StorageEngineException {
        logger.info("");
        logger.info("");
        logger.info("####################################################");
        logger.info("########## Testing query " + query.toJson());
        logger.info("####################################################");
        Assert.assertTrue(dbQueryExecutor.canUseThisExecutor(query, options));

        ParsedVariantQuery variantQuery = variantStorageEngine.parseQuery(query, options);
        VariantQueryResult<Variant> expected = dbQueryExecutor.get(new Query(variantQuery.getQuery()), new QueryOptions(options));

        VariantQueryResult<Variant> unfilteredResult = null;
        VariantQueryResult<Variant> result = null;
        if (matcher != null) {
            Query emptyQuery = new Query();
            List<String> fileNames = new LinkedList<>();
            List<String> sampleNames = new LinkedList<>();
            List<String> studyNames = new LinkedList<>();
            emptyQuery.put(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.NONE);
            emptyQuery.put(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.NONE);
            emptyQuery.put(VariantQueryParam.INCLUDE_STUDY.key(), VariantQueryUtils.NONE);
            emptyQuery.putIfNotNull(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), query.get(VariantQueryParam.INCLUDE_SAMPLE_ID.key()));
            emptyQuery.putIfNotNull(VariantQueryParam.INCLUDE_GENOTYPE.key(), query.get(VariantQueryParam.INCLUDE_GENOTYPE.key()));
            emptyQuery.putIfNotNull(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), query.get(VariantQueryParam.INCLUDE_SAMPLE_DATA.key()));
            for (VariantQueryProjection.StudyVariantQueryProjection study : variantQuery.getProjection().getStudies().values()) {
                studyNames.add(study.getStudyMetadata().getName());
                for (Integer file : study.getFiles()) {
                    String fileName = metadataManager.getFileName(study.getStudyMetadata().getId(), file);
                    fileNames.add(fileName);
                }
                for (Integer sample : study.getSamples()) {
                    String sampleName = metadataManager.getSampleName(study.getStudyMetadata().getId(), sample);
                    sampleNames.add(sampleName);
                }
            }
            if (!studyNames.isEmpty()) {
                emptyQuery.put(VariantQueryParam.INCLUDE_STUDY.key(), studyNames);
            }
            if (!fileNames.isEmpty()) {
                emptyQuery.put(VariantQueryParam.INCLUDE_FILE.key(), fileNames);
            }
            if (!sampleNames.isEmpty()) {
                emptyQuery.put(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNames);
            }
            QueryOptions emptyOptions = new QueryOptions();
            emptyOptions.putIfNotEmpty(QueryOptions.INCLUDE, options.getString(QueryOptions.INCLUDE));
            emptyOptions.putIfNotEmpty(QueryOptions.EXCLUDE, options.getString(QueryOptions.EXCLUDE));
            unfilteredResult = dbQueryExecutor.get(emptyQuery, emptyOptions);
        }

        for (VariantQueryExecutor variantQueryExecutor : variantQueryExecutors) {
            if (variantQueryExecutor.canUseThisExecutor(query, options)) {
                logger.info("##########################");
                logger.info("########## Testing " + variantQueryExecutor.getClass().getSimpleName() + " with query " + query.toJson());
                result = variantQueryExecutor.get(new Query(variantQuery.getQuery()), new QueryOptions(options));
                logger.info("########## Num results : " + result.getNumResults());
                logger.info("##########################");
                Assert.assertEquals(expected.getResults(), result.getResults());

                assertThat(result, numResults(gt(0)));

                if (matcher != null) {
                    assertThat(result, everyResult(unfilteredResult, matcher));
                }
            }
        }
        // Return any result.
        return result;
    }

}
