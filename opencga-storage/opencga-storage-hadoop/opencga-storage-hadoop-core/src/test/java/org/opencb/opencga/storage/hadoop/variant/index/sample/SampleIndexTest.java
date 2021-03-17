package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationConstants;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexVariantAggregationExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.SampleIndexAnnotationLoaderDriver;
import org.opencb.opencga.storage.hadoop.variant.index.family.FamilyIndexDriver;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationConstants.THREE_PRIME_UTR_VARIANT;

/**
 * Created on 12/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    private VariantHadoopDBAdaptor dbAdaptor;
    private static boolean loaded = false;
    public static final String STUDY_NAME_3 = "study_3";
    private static final List<String> studies = Arrays.asList(STUDY_NAME, STUDY_NAME_2, STUDY_NAME_3);
    private static final Map<String, List<String>> sampleNames = new HashMap<String, List<String>>() {{
        put(STUDY_NAME, Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
        put(STUDY_NAME_2, Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
        put(STUDY_NAME_3, Arrays.asList("NA12877", "NA12878"));
    }};
//    private static List<List<String>> trios = Arrays.asList(
//            Arrays.asList("NA19600", "NA19660", "NA19661"),
//            Arrays.asList("NA19660", "NA19661", "NA19685"),
//            Arrays.asList("NA19661", "NA19685", "NA19600"),
//            Arrays.asList("NA19685", "NA19600", "NA19660")
//    );
    private static List<List<String>> trios = Arrays.asList(
            Arrays.asList("NA19660", "NA19661", "NA19685"),
            Arrays.asList("NA19660", "NA19661", "NA19600")
    );

    @Before
    public void before() throws Exception {
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        if (!loaded) {
            load();
            loaded = true;
        }
    }

    public void load() throws Exception {
        clearDB(DB_NAME);

        HadoopVariantStorageEngine engine = getVariantStorageEngine();

        // Study 1 - single file
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, smallInputUri, outputUri, params, true, true, true);
        engine.familyIndex(STUDY_NAME, trios, new ObjectMap());

        // Study 2 - multi files
        params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_2)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI);

        int version = metadataManager.addSampleIndexConfiguration(STUDY_NAME_2, SampleIndexConfiguration.defaultConfiguration()
                .addFileIndexField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.SAMPLE, "DS", new double[]{0, 1, 2}))).getVersion();
        System.out.println("version = " + version);

        runETL(engine, getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        engine.familyIndex(STUDY_NAME_2, trios, new ObjectMap());

        // Study 3 - platinum
        params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_3)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getPlatinumFile(0), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(1), outputUri, params, true, true, true);

        this.variantStorageEngine.annotate(new Query(), new QueryOptions(DefaultVariantAnnotationManager.OUT_DIR, outputUri));

        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
    }

    @Test
    public void checkLoadedData() throws Exception {
        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        int studyId = variantStorageEngine.getMetadataManager().getStudyId(STUDY_NAME);
        SampleIndexVariantBiConverter converter = new SampleIndexVariantBiConverter(SampleIndexSchema.defaultSampleIndexSchema());
        Iterator<SampleMetadata> it = variantStorageEngine.getMetadataManager().sampleMetadataIterator(studyId);
        while (it.hasNext()) {
            SampleMetadata sample = it.next();
            Iterator<SampleIndexEntry> indexIt = variantStorageEngine.getSampleIndexDBAdaptor().rawIterator(studyId, sample.getId());
            while (indexIt.hasNext()) {
                SampleIndexEntry record = indexIt.next();

                List<Variant> variants = variantStorageEngine.getDBAdaptor().get(new Query()
                        .append(VariantQueryParam.STUDY.key(), STUDY_NAME)
                        .append(VariantQueryParam.SAMPLE.key(), sample.getName())
                        .append(VariantQueryParam.REGION.key(),
                                record.getChromosome() + ":" + record.getBatchStart() + "-" + (record.getBatchStart() + SampleIndexSchema.BATCH_SIZE - 1)), null).getResults();

                Map<String, List<String>> gtsMap = variants.stream()
                        .collect(groupingBy(v -> v.getStudies().get(0).getSampleData(0).get(0), mapping(Variant::toString, toList())));
//                System.out.println("record = " + record);
                gtsMap.keySet().removeIf(gt -> !gt.contains("1"));

                Assert.assertEquals(gtsMap.keySet(), record.getGts().keySet());
                for (Map.Entry<String, SampleIndexEntry.SampleIndexGtEntry> entry : record.getGts().entrySet()) {
                    String gt = entry.getKey();
                    List<String> expectedVariants = gtsMap.get(gt);
                    List<String> actualVariants;
                    if (entry.getValue().getVariants() == null) {
                        actualVariants = Collections.emptyList();
                    } else {
                        actualVariants = Lists.newArrayList(converter.toVariantsIterator(entry.getValue()))
                                .stream()
                                .map(Variant::toString)
                                .collect(toList());
                    }
                    Assert.assertEquals(gt, expectedVariants, actualVariants);
                }

            }
        }

    }

    @Test
    public void regenerateSampleIndex() throws Exception {
        for (String study : studies) {
            int studyId = dbAdaptor.getMetadataManager().getStudyId(study);
            int version = dbAdaptor.getMetadataManager().getStudyMetadata(studyId).getSampleIndexConfigurationLatest().getVersion();
            String orig = dbAdaptor.getTableNameGenerator().getSampleIndexTableName(studyId, version);
            String copy = orig + "_copy";

            dbAdaptor.getHBaseManager().createTableIfNeeded(copy, Bytes.toBytes(GenomeHelper.COLUMN_FAMILY),
                    Compression.Algorithm.NONE);

            ObjectMap options = new ObjectMap()
                    .append(SampleIndexDriver.OUTPUT, copy)
                    .append(SampleIndexDriver.SAMPLES, "all");
            new TestMRExecutor().run(SampleIndexDriver.class, SampleIndexDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    Collections.emptySet(), options), options);

            new TestMRExecutor().run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    Collections.emptySet(), options), options);

            if (sampleNames.get(study).containsAll(trios.get(0))) {
                options.put(FamilyIndexDriver.TRIOS, trios.stream().map(trio -> String.join(",", trio)).collect(Collectors.joining(";")));
                options.put(FamilyIndexDriver.OVERWRITE, true);
                new TestMRExecutor().run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
                        dbAdaptor.getArchiveTableName(studyId),
                        dbAdaptor.getVariantTable(),
                        studyId,
                        Collections.emptySet(), options), options);
            }

            Connection c = dbAdaptor.getHBaseManager().getConnection();

            VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), studyId, copy);

            ResultScanner origScanner = c.getTable(TableName.valueOf(orig)).getScanner(new Scan());
            ResultScanner copyScanner = c.getTable(TableName.valueOf(copy)).getScanner(new Scan());
            while (true) {
                Result origValue = origScanner.next();
                Result copyValue = copyScanner.next();
                if (origValue == null) {
                    assertNull(copyValue);
                    break;
                }
                NavigableMap<byte[], byte[]> origFamily = origValue.getFamilyMap(GenomeHelper.COLUMN_FAMILY_BYTES);
                NavigableMap<byte[], byte[]> copyFamily = copyValue.getFamilyMap(GenomeHelper.COLUMN_FAMILY_BYTES);

                String row = SampleIndexSchema.rowKeyToString(origValue.getRow());
                assertEquals(row, origFamily.keySet().stream().map(Bytes::toString).collect(toList()), copyFamily.keySet().stream().map(Bytes::toString).collect(toList()));
                assertEquals(row, origFamily.size(), copyFamily.size());

                for (byte[] key : origFamily.keySet()) {
                    byte[] expecteds = origFamily.get(key);
                    byte[] actuals = copyFamily.get(key);
                    try {
                        assertArrayEquals(row + " " + Bytes.toString(key), expecteds, actuals);
                    } catch (AssertionError error) {
                        System.out.println("Expected " + IndexUtils.bytesToString(expecteds));
                        System.out.println("actuals " + IndexUtils.bytesToString(actuals));
                        throw error;
                    }
                }
            }
        }



//        VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), copy);

    }

    public VariantQueryResult<Variant> dbAdaptorQuery(Query query, QueryOptions options) {
        query = variantStorageEngine.preProcessQuery(query, options);
        return dbAdaptor.get(query, options);
    }

    @Test
    public void testQueryFileIndex() throws Exception {
        testQueryFileIndex(new Query(TYPE.key(), "SNV"));
//        testQueryFileIndex(new Query(TYPE.key(), "SNP"));
        testQueryFileIndex(new Query(TYPE.key(), "INDEL"));
        testQueryFileIndex(new Query(TYPE.key(), "SNV,INDEL"));
        testQueryFileIndex(new Query(FILTER.key(), "PASS"));
        testQueryFileIndex(new Query(TYPE.key(), "INDEL").append(FILTER.key(), "PASS"));
        testQueryFileIndex(new Query(QUAL.key(), ">=30").append(FILTER.key(), "PASS"));
        testQueryFileIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"));
        testQueryIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"), new Query(STUDY.key(), STUDY_NAME).append(SAMPLE.key(), "NA19660,NA19661"));
        testQueryIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"), new Query(STUDY.key(), STUDY_NAME).append(SAMPLE.key(), "NA19600,NA19661"));
        testQueryIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"), new Query(STUDY.key(), STUDY_NAME).append(GENOTYPE.key(), "NA19660:0/1;NA19661:0/0,0|0"));
        testQueryIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"), new Query(STUDY.key(), STUDY_NAME).append(GENOTYPE.key(), "NA19600:0/1;NA19661:0/0,0|0"));

        testQueryIndex(new Query(FILE.key(), "chr22_1-2-DUP.variant-test-file.vcf.gz").append(FILTER.key(), "PASS"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600"));

        testQueryIndex(new Query(FILE.key(), "chr22_1-2-DUP.variant-test-file.vcf.gz").append(FILTER.key(), "PASS"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(GENOTYPE.key(), "NA19600:0/1;NA19661:0/0"));

        testQueryIndex(new Query(FILE.key(), "chr22_1-2.variant-test-file.vcf.gz").append(FILTER.key(), "PASS"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(GENOTYPE.key(), "NA19600:0/1;NA19661:0/0"));
    }

    @Test
    public void testMultiFileFilters() throws Exception {
        testQueryIndex(new Query(FILE_DATA.key(), "chr22_1-1.variant-test-file.vcf.gz:FILTER=PASS,chr22_1-2-DUP.variant-test-file.vcf.gz:FILTER=PASS"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600"));

        testQueryIndex(new Query(FILE_DATA.key(), "chr22_1-2-DUP.variant-test-file.vcf.gz:FILTER=FilterA"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600"));

        testQueryIndex(new Query(FILE_DATA.key(), "chr22_1-2-DUP.variant-test-file.vcf.gz:FILTER=FilterB"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600"));

        testQueryIndex(new Query(FILE_DATA.key(), "chr22_1-2.variant-test-file.vcf.gz:FILTER=FilterA,chr22_1-2-DUP.variant-test-file.vcf.gz:FILTER=FilterA"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600"));

        testQueryIndex(new Query(FILE_DATA.key(), "chr22_1-2.variant-test-file.vcf.gz:FILTER=PASS,chr22_1-2-DUP.variant-test-file.vcf.gz:FILTER=PASS"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600"));

        testQueryIndex(new Query(FILE_DATA.key(), "chr22_1-2.variant-test-file.vcf.gz:FILTER=PASS;chr22_1-2-DUP.variant-test-file.vcf.gz:FILTER=PASS"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600"));

        // 22:20780030:-:C
        testQueryIndex(new Query(FILE_DATA.key(), "chr22_1-2.variant-test-file.vcf.gz:FILTER=PASS,chr22_1-2-DUP.variant-test-file.vcf.gz:FILTER=PASS"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600")
                        .append(SAMPLE_DATA.key(), "NA19600:DS=2.005"));

        // 22:36591380::A:G
        testQueryIndex(new Query(FILE_DATA.key(), "chr22_1-2.variant-test-file.vcf.gz:FILTER=FilterB,chr22_1-2-DUP.variant-test-file.vcf.gz:FILTER=FilterB"),
                new Query()
                        .append(STUDY.key(), STUDY_NAME_2)
                        .append(SAMPLE.key(), "NA19600")
                        .append(SAMPLE_DATA.key(), "NA19600:DS=1.005"));
    }

    @Test
    public void testQueryAnnotationIndex() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_BIOTYPE.key(), "protein_coding"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained").append(GENE.key(), "HPS4"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant").append(ANNOT_BIOTYPE.key(), "nonsense_mediated_decay"));
        testQueryAnnotationIndex(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=tolerated"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.001"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL>0.005"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL>0.008"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL>0.005,GNOMAD_GENOMES:ALL>0.005"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "pathogenic"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "likely_benign"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "pathogenic,likely_benign"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "benign"));

//        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "intergenic_variant"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained,mature_miRNA_variant"));


        //    11:62951221:C:G
        // - SLC22A25 : [missense_variant, stop_lost, 3_prime_UTR_variant, NMD_transcript_variant]
        // - SLC22A10 : [non_coding_transcript_variant, intron_variant]

        // Should return the variant     // 11:62951221:C:G
        testQueryAnnotationIndex(new Query().append(GENE.key(), "SLC22A25").append(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));
        // Should NOT return the variant // 11:62951221:C:G
        testQueryAnnotationIndex(new Query().append(GENE.key(), "SLC22A10").append(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));
    }

    public void testQueryAnnotationIndex(Query annotationQuery) throws Exception {
        for (String study : studies) {
            for (String sampleName : sampleNames.get(study)) {
                SampleIndexQuery sampleIndexQuery = testQueryIndex(annotationQuery, new Query()
                        .append(STUDY.key(), study)
                        .append(SAMPLE.key(), sampleName));
                assertTrue(!sampleIndexQuery.emptyAnnotationIndex() || !sampleIndexQuery.getAnnotationIndexQuery().getPopulationFrequencyQueries().isEmpty());
            }
        }
    }

    public void testQueryFileIndex(Query query) throws Exception {
        for (String study : studies) {
            for (String sampleName : sampleNames.get(study)) {
                testQueryIndex(query, new Query()
                        .append(STUDY.key(), study)
                        .append(SAMPLE.key(), sampleName));
            }
        }
    }

    public SampleIndexQuery testQueryIndex(Query testQuery, Query query) throws Exception {
        System.out.println("----- START TEST QUERY INDEX -----------------------------------------------------");
        System.out.println("testQuery  = " + testQuery.toJson());
        System.out.println("query      = " + query.toJson());
        System.out.println("--------");

        // Query DBAdaptor
        System.out.println("#Query DBAdaptor");
        query.putAll(testQuery);
        VariantQueryResult<Variant> queryResult = dbAdaptorQuery(new Query(query), new QueryOptions());
        int onlyDBAdaptor = queryResult.getNumResults();

        // Query SampleIndex
        System.out.println("#Query SampleIndex");
        SampleIndexDBAdaptor sampleIndexDBAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor();
        Query sampleIndexVariantQuery = variantStorageEngine.preProcessQuery(query, new QueryOptions());
        SampleIndexQuery indexQuery = sampleIndexDBAdaptor.parseSampleIndexQuery(sampleIndexVariantQuery);
//        int onlyIndex = (int) ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor()
//                .count(indexQuery, "NA19600");
        DataResult<Variant> result = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor()
                .iterator(indexQuery).toDataResult();
        System.out.println("result.getResults() = " + result.getResults());
        int onlyIndex = result.getNumResults();

        // Query SampleIndex+DBAdaptor
        System.out.println("#Query SampleIndex+DBAdaptor");
        queryResult = variantStorageEngine.get(new Query(query), new QueryOptions());
        int indexAndDBAdaptor = queryResult.getNumResults();
        long indexAndDBAdaptorMatches = queryResult.getNumMatches();
        assertEquals(indexAndDBAdaptorMatches, indexAndDBAdaptor);

        System.out.println("queryResult.source = " + queryResult.getSource());

        System.out.println("--- RESULTS -----");
        System.out.println("testQuery          = " + testQuery.toJson());
        System.out.println("query              = " + query.toJson());
        System.out.println("dbAdaptorQuery     = " + sampleIndexVariantQuery.toJson());
        System.out.println("Native dbAdaptor   = " + VariantHBaseQueryParser.isSupportedQuery(sampleIndexVariantQuery) + " -> " + VariantHBaseQueryParser.unsupportedParamsFromQuery(sampleIndexVariantQuery));
        System.out.println("annotationIndex    = " + IndexUtils.maskToString(indexQuery.getAnnotationIndexMask(), indexQuery.getAnnotationIndex()));
        System.out.println("biotypeMask        = " + IndexUtils.byteToString(indexQuery.getAnnotationIndexQuery().getBiotypeMask()));
        System.out.println("ctMask             = " + IndexUtils.shortToString(indexQuery.getAnnotationIndexQuery().getConsequenceTypeMask()));
        System.out.println("clinicalMask       = " + IndexUtils.byteToString(indexQuery.getAnnotationIndexQuery().getClinicalMask()));
//        for (String sample : indexQuery.getSamplesMap().keySet()) {
//            System.out.println("fileIndex("+sample+") = " + IndexUtils.maskToString(indexQuery.getFileIndexMask(sample), indexQuery.getFileIndex(sample)));
//        }
        System.out.println("Query SampleIndex             = " + onlyIndex);
        System.out.println("Query DBAdaptor               = " + onlyDBAdaptor);
        System.out.println("Query SampleIndex+DBAdaptor   = " + indexAndDBAdaptor);
        System.out.println("--------");

        System.out.println("dbAdaptorQuery(new Query(query), new QueryOptions()); = " + dbAdaptorQuery(new Query(query), new QueryOptions()).getResults().stream().map(Variant::toString).sorted().collect(Collectors.toList()));
        if (onlyDBAdaptor != indexAndDBAdaptor) {
            VariantQueryResult<Variant> queryResultAux = variantStorageEngine.get(new Query(query), new QueryOptions());
            List<String> indexAndDB = queryResultAux.getResults().stream().map(Variant::toString).sorted().collect(Collectors.toList());
            queryResultAux = dbAdaptorQuery(new Query(query), new QueryOptions());
            List<String> noIndex = queryResultAux.getResults().stream().map(Variant::toString).sorted().collect(Collectors.toList());

            for (String s : indexAndDB) {
                if (!noIndex.contains(s)) {
                    System.out.println("From SampleIndex+DB, not in DBAdaptor = " + s);
                }
            }

            for (String s : noIndex) {
                if (!indexAndDB.contains(s)) {
                    System.out.println("From DBAdaptor, not in SampleIndex+DB = " + s);
                }
            }
        }
        assertEquals(onlyDBAdaptor, indexAndDBAdaptor);
        assertThat(queryResult, numResults(lte(onlyIndex)));
//        assertThat(queryResult, numResults(gt(0)));
        return indexQuery;
    }

    @Test
    public void testSampleIndexSkipIntersect() throws StorageEngineException {
        Query query = new Query(VariantQueryParam.SAMPLE.key(), sampleNames.get(STUDY_NAME).get(0)).append(VariantQueryParam.STUDY.key(), STUDY_NAME);
        VariantQueryResult<Variant> result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_SET)));
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET))).append(TYPE.key(), VariantType.INDEL);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(2, 4)));
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(2, 4)))
                .append(ANNOT_BIOTYPE.key(), VariantAnnotationConstants.PROTEIN_CODING);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(2, 4)) + "," + THREE_PRIME_UTR_VARIANT)
                .append(ANNOT_BIOTYPE.key(), VariantAnnotationConstants.PROTEIN_CODING);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertNotEquals("sample_index_table", result.getSource());
    }

    @Test
    public void testCount() throws StorageEngineException {
        /*
            6    31_000_000
            6    32_000_000
            8    144_000_000
            9    139_000_000
            19   14_000_000
         */
        List<Query> queries = Arrays.asList(
                new Query(),
                new Query(REGION.key(), "chr1").append(ANNOT_BIOTYPE.key(), "protein_coding"),
                new Query(REGION.key(), Arrays.asList(new Region("22", 36591300, 46000000), new Region("1", 1000, 16400000))),
                new Query(REGION.key(), Arrays.asList(
                        new Region("8", 144_671_680, 144_690_000),
                        new Region("8", 144_700_000, 144_995_738))),
                new Query(REGION.key(), Arrays.asList(
                        new Region("8", 144_671_680, 144_690_000),
                        new Region("8", 144_700_000, 144_995_738),
                        new Region("8", 145_100_000, 146_100_000))),
                new Query(REGION.key(), Arrays.asList(
                        new Region("6", 31_200_000, 31_800_000),
                        new Region("6", 33_200_000, 34_800_000),
                        new Region("8", 144_671_680, 144_690_000),
                        new Region("8", 144_700_000, 144_995_738),
                        new Region("8", 145_100_000, 146_100_000)))
        );

        for (String study : studies) {
            for (String sampleName : sampleNames.get(study)) {
                for (Query baseQuery : queries) {
                    System.out.println("-----------------------------------");
                    System.out.println("study = " + study);
                    System.out.println("sampleName = " + sampleName);
                    System.out.println("baseQuery = " + baseQuery.toJson());
                    StopWatch stopWatch = StopWatch.createStarted();
                    Query query = new Query(baseQuery)
                            .append(VariantQueryParam.STUDY.key(), study)
                            .append(GENOTYPE.key(), sampleName + ":1|0,0|1,1|1");
                    SampleIndexDBAdaptor sampleIndexDBAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor();
                    long actualCount = sampleIndexDBAdaptor.count(sampleIndexDBAdaptor.parseSampleIndexQuery(new Query(query)));

                    System.out.println("---");
                    System.out.println("Count indexTable " + stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000.0);
                    System.out.println("Count = " + actualCount);

                    stopWatch = StopWatch.createStarted();
                    long actualCountIterator = sampleIndexDBAdaptor.iterator(sampleIndexDBAdaptor.parseSampleIndexQuery(new Query(query))).toDataResult().getNumResults();
                    System.out.println("---");
                    System.out.println("Count indexTable iterator " + stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000.0);
                    System.out.println("Count = " + actualCountIterator);
                    stopWatch = StopWatch.createStarted();
                    long expectedCount = dbAdaptor.count(query).first();
                    System.out.println("Count variants   " + stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000.0);
                    System.out.println("Count = " + expectedCount);
                    System.out.println("-----------------------------------");
                    assertEquals(expectedCount, actualCount);
                    assertEquals(expectedCount, actualCountIterator);
                }
            }
        }
    }

    @Test
    public void testAggregation() throws Exception {
        SampleIndexVariantAggregationExecutor executor = new SampleIndexVariantAggregationExecutor(metadataManager, ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor());

        testAggregation(executor, "qual", STUDY_NAME_3, "NA12877");
        testAggregation(executor, "dp", STUDY_NAME_3, "NA12877");
        testAggregation(executor, "qual>>type", STUDY_NAME_3, "NA12877");
        testAggregation(executor, "type>>qual", STUDY_NAME_3, "NA12877");

        testAggregation(executor, "chromosome>>type>>ct");
        testAggregation(executor, "type>>ct");
        testAggregation(executor, "type;gt>>ct");
        testAggregation(executor, "gt>>type>>ct>>biotype");
        testAggregation(executor, "clinicalSignificance>>gt>>type>>ct>>biotype");
    }

    private void testAggregation(SampleIndexVariantAggregationExecutor executor, String facet) throws JsonProcessingException {
        String study = studies.get(0);
        testAggregation(executor, facet, study, sampleNames.get(study).get(0));
    }

    private void testAggregation(SampleIndexVariantAggregationExecutor executor, String facet, String study, String sample) throws JsonProcessingException {
        Query query = new Query(STUDY.key(), study).append(SAMPLE.key(), sample);
        assertTrue(executor.canUseThisExecutor(query, new QueryOptions(QueryOptions.FACET, facet)));

        VariantQueryResult<FacetField> result = executor.aggregation(query,
                new QueryOptions(QueryOptions.FACET, facet));


        System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result.first()));
    }

    @Test
    public void testSampleVariantStats() throws Exception {
        for (String study : studies) {
            for (String sample : sampleNames.get(study)) {
                DataResult<SampleVariantStats> result = variantStorageEngine.sampleStatsQuery(study, sample, null);
                System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result.first()));
            }
        }
    }


    @Test
    public void testApproximateCount() {
        VariantQueryResult<Variant> result = variantStorageEngine.get(
                new Query()
                        .append(STUDY.key(), STUDY_NAME)
                        .append(SAMPLE.key(), sampleNames.get(STUDY_NAME).get(0))
                        .append(INCLUDE_SAMPLE_ID.key(), "true")
                        .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.9"),
                new QueryOptions()
                        .append(QueryOptions.LIMIT, 10)
                        .append(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), 200)
                        .append(QueryOptions.COUNT, true));

        assertTrue(result.getApproximateCount());
        assertThat(result.getApproximateCountSamplingSize(), gte(200));
        assertEquals("hadoop + sample_index_table", result.getSource());
    }

}