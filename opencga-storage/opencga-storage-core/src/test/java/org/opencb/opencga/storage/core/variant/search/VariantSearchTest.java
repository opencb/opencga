package org.opencb.opencga.storage.core.variant.search;

import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.lang.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.Score;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageTest;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

public class VariantSearchTest extends VariantStorageBaseTest implements DummyVariantStorageTest {

    @Rule
    public VariantSolrExternalResource solr = new VariantSolrExternalResource();

    @Test
    public void testTranscriptInfo() throws Exception {
        int limit = 500;

        VariantStorageMetadataManager scm = variantStorageEngine.getMetadataManager();

        solr.configure(variantStorageEngine);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();

        System.out.println(smallInputUri.getPath());

        List<Variant> variants = getVariants(limit);
        List<Variant> annotatedVariants = annotatedVariants(variants);

        metadataManager.createStudy("s1");

        String collection = solr.coreName;
        variantSearchManager.createCore(collection, VariantSearchManager.CONF_SET);

        variantSearchManager.insert(collection, annotatedVariants);

        VariantQueryResult<Variant> results = variantSearchManager.query(collection, new Query(),
                new QueryOptions(QueryOptions.LIMIT, limit));

        for (int i = 0; i < limit; i++) {
            Map<String, ConsequenceType> inMap = getConsequenceTypeMap(annotatedVariants.get(i));
            Map<String, ConsequenceType> outMap = getConsequenceTypeMap(results.getResult().get(i));

            System.out.println(inMap.size() + " vs " + outMap.size());
            assert(inMap.size() == outMap.size());
            for (String key: inMap.keySet()) {
                ConsequenceType inCT = inMap.get(key);
                ConsequenceType outCT = outMap.get(key);

                // Check biotype
                System.out.println(inCT.getBiotype() + " vs " + outCT.getBiotype());
                assert(inCT.getBiotype().equals(outCT.getBiotype()));

                // Check annotation flags
                System.out.println("inCT, annotation flags:");
                if (ListUtils.isNotEmpty(inCT.getTranscriptAnnotationFlags())) {
                    System.out.println("\t" + StringUtils.join(inCT.getTranscriptAnnotationFlags(), ","));
                }
                System.out.println();
                System.out.println("outCT, annotation flags:");
                if (ListUtils.isNotEmpty(outCT.getTranscriptAnnotationFlags())) {
                    System.out.println("\t" + StringUtils.join(outCT.getTranscriptAnnotationFlags(), ","));
                }
                System.out.println();
                if (ListUtils.isNotEmpty(inCT.getTranscriptAnnotationFlags())
                        && ListUtils.isNotEmpty(outCT.getTranscriptAnnotationFlags())) {
                    if (inCT.getTranscriptAnnotationFlags().size() == outCT.getTranscriptAnnotationFlags().size()) {
                        for (int j = 0; j < inCT.getTranscriptAnnotationFlags().size(); j++) {
                            if (!inCT.getTranscriptAnnotationFlags().get(j)
                                    .equals(outCT.getTranscriptAnnotationFlags().get(j))) {
                                fail("Annotation flags mismatch: " + inCT.getTranscriptAnnotationFlags().get(j) + " vs "
                                        + outCT.getTranscriptAnnotationFlags().get(j));
                            }
                        }
                    } else {
                        fail("Annotation flags mismatch (size)");
                    }
                } else if (ListUtils.isNotEmpty(inCT.getTranscriptAnnotationFlags())
                        || ListUtils.isNotEmpty(outCT.getTranscriptAnnotationFlags())) {
                    fail("Annotation flags mismatch");
                }

                // Check cdnaPosition, cdsPostion and codon
                int inCdnaPosition =  inCT.getCdnaPosition() == null ? 0 : inCT.getCdnaPosition();
                int inCdsPosition =  inCT.getCdsPosition() == null ? 0 : inCT.getCdsPosition();
                int outCdnaPosition =  outCT.getCdnaPosition() == null ? 0 : outCT.getCdnaPosition();
                int outCdsPosition =  outCT.getCdsPosition() == null ? 0 : outCT.getCdsPosition();
                String inCodon =  inCT.getCodon() == null ? "" : inCT.getCodon().trim();
                String outCodon =  outCT.getCodon() == null ? "" : outCT.getCodon().trim();
                System.out.println(inCdnaPosition + " vs " + outCdnaPosition
                        + " ; " + inCdsPosition + " vs " + outCdsPosition
                        + " ; " + inCodon + " vs " + outCodon);
                assert(inCdnaPosition == outCdnaPosition);
                assert(inCdsPosition == outCdsPosition);
                assert(inCodon.equals(outCodon));

                if (inCT.getProteinVariantAnnotation() != null && outCT.getProteinVariantAnnotation() != null) {
                    // Check sift and polyphen values
                    checkScore(inCT.getProteinVariantAnnotation().getSubstitutionScores(), outCT.getProteinVariantAnnotation().getSubstitutionScores(), "sift");
                    checkScore(inCT.getProteinVariantAnnotation().getSubstitutionScores(), outCT.getProteinVariantAnnotation().getSubstitutionScores(), "polyphen");

                    String inUniprotAccession = inCT.getProteinVariantAnnotation().getUniprotAccession() == null ? "" : inCT.getProteinVariantAnnotation().getUniprotAccession();
                    String outUniprotAccession = outCT.getProteinVariantAnnotation().getUniprotAccession() == null ? "" : outCT.getProteinVariantAnnotation().getUniprotAccession();
                    String inUniprotName = inCT.getProteinVariantAnnotation().getUniprotName() == null ? "" : inCT.getProteinVariantAnnotation().getUniprotName();
                    String outUniprotName = outCT.getProteinVariantAnnotation().getUniprotName() == null ? "" : outCT.getProteinVariantAnnotation().getUniprotName();
                    String inUniprotVariantId = inCT.getProteinVariantAnnotation().getUniprotVariantId() == null ? "" : inCT.getProteinVariantAnnotation().getUniprotVariantId();
                    String outUniprotVariantId = outCT.getProteinVariantAnnotation().getUniprotVariantId() == null ? "" : outCT.getProteinVariantAnnotation().getUniprotVariantId();
                    System.out.println(inUniprotAccession + " vs " + outUniprotAccession
                            + " ; " + inUniprotName + " vs " + outUniprotName
                            + " ; " + inUniprotVariantId + " vs " + outUniprotVariantId);
                    assert(inUniprotAccession.equals(outUniprotAccession));
                    assert(inUniprotName.equals(outUniprotName));
                    assert(inUniprotVariantId.equals(outUniprotVariantId));


                    int inPosition =  inCT.getProteinVariantAnnotation().getPosition() == null ? 0 : inCT.getProteinVariantAnnotation().getPosition();
                    int outPosition =  outCT.getProteinVariantAnnotation().getPosition() == null ? 0 : outCT.getProteinVariantAnnotation().getPosition();
                    String inRef = inCT.getProteinVariantAnnotation().getReference() == null ? "" : inCT.getProteinVariantAnnotation().getReference();
                    String outRef = outCT.getProteinVariantAnnotation().getReference() == null ? "" : outCT.getProteinVariantAnnotation().getReference();
                    String inAlt = inCT.getProteinVariantAnnotation().getAlternate() == null ? "" : inCT.getProteinVariantAnnotation().getAlternate();
                    String outAlt = outCT.getProteinVariantAnnotation().getAlternate() == null ? "" : outCT.getProteinVariantAnnotation().getAlternate();
                    System.out.println(inPosition + " vs " + outPosition
                            + " ; " + inRef + " vs " + outRef
                            + " ; " + inAlt + " vs " + outAlt);
                    assert(inPosition == outPosition);
                    assert(inRef.equals(outRef));
                    assert(inAlt.equals(outAlt));
                } else if (inCT.getProteinVariantAnnotation() == null && outCT.getProteinVariantAnnotation() == null) {
                    continue;
                } else {
                    fail("Mismatch protein variant annotation");
                }
            }
        }

        System.out.println("#variants = " + variants.size());
        System.out.println("#annotations = " + annotatedVariants.size());
        System.out.println("#variants from Solr = " + results.getResult().size());
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

        String studyId = "abyu12";
        String fileId = "a.vcf";

        variants.get(0).getStudies().get(0).getFiles().get(0).setFileId(fileId);
        System.out.println(variants.get(0).getStudies().get(0).getFiles().get(0).getFileId());
        //System.exit(-1);

        scm.createStudy(studyId);

        String collection = solr.coreName;
        variantSearchManager.createCore(collection, VariantSearchManager.CONF_SET);

        LinkedHashMap<String, Integer> samplePosition = new LinkedHashMap<>();
        samplePosition.put("A-A", 0);
        samplePosition.put("B", 1);
        samplePosition.put("C", 2);
        samplePosition.put("D", 3);
        annotatedVariants.get(0).getStudies().get(0).setStudyId(studyId).setSortedSamplesPosition(samplePosition);
        variantSearchManager.insert(collection, annotatedVariants);

        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), studyId);
//        query.put(VariantQueryParam.SAMPLE.key(), samplePosition.keySet().toArray()[0]);
        query.put(VariantQueryParam.FILE.key(), fileId);
        query.put(VariantQueryParam.FILTER.key(), "PASS");
        query.put(VariantQueryParam.ANNOT_CLINICAL_SIGNIFICANCE.key(), "benign");
        VariantQueryResult<Variant> results = variantSearchManager.query(collection, query,
                new QueryOptions(QueryOptions.LIMIT, limit));

        if (results.getResult().size() > 0) {
            System.out.println(results.getResult().get(0).toJson());
        } else {
            System.out.println("Not found!!!!");
        }
    }

    private Map<String, ConsequenceType> getConsequenceTypeMap (Variant variant){
        Map<String, ConsequenceType> map = new HashMap<>();
        if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
            for (ConsequenceType consequenceType: variant.getAnnotation().getConsequenceTypes()) {
                if (StringUtils.isNotEmpty(consequenceType.getEnsemblTranscriptId())) {
                    map.put(consequenceType.getEnsemblTranscriptId(), consequenceType);
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
            double inValue = inScore.getScore() == null ? 0 : inScore.getScore();
            double outValue = outScore.getScore() == null ? 0 : outScore.getScore();
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
        CellBaseClient cellBaseClient = new CellBaseClient(variantStorageEngine.getConfiguration().getCellbase().toClientConfiguration());
        QueryResponse<VariantAnnotation> queryResponse = cellBaseClient.getVariantClient().getAnnotationByVariantIds(variants.stream().map(Variant::toString).collect(Collectors.toList()), QueryOptions.empty());

        // Set annotations
        for (int i = 0; i < variants.size(); i++) {
            variants.get(i).setAnnotation(queryResponse.getResponse().get(i).first());
        }
        return variants;
    }
}
