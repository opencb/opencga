package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
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
import org.opencb.opencga.core.models.variant.VariantAnnotationConstants;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.query.Values;
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
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalMultiValuedIndexField;
import org.opencb.opencga.storage.hadoop.variant.index.family.FamilyIndexDriver;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleFileIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static org.junit.Assert.*;
import static org.opencb.opencga.core.models.variant.VariantAnnotationConstants.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 12/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    private VariantHadoopDBAdaptor dbAdaptor;
    private SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private static boolean loaded = false;
    public static final String STUDY_NAME_3 = "study_3";
    public static final String STUDY_NAME_4 = "study_4";
    private static final List<String> studies = Arrays.asList(STUDY_NAME, STUDY_NAME_2, STUDY_NAME_3, STUDY_NAME_4);
    private static final Map<String, List<String>> sampleNames = new HashMap<String, List<String>>() {{
        put(STUDY_NAME, Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
        put(STUDY_NAME_2, Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
        put(STUDY_NAME_3, Arrays.asList("NA12877", "NA12878"));
        put(STUDY_NAME_4, Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
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
    private static List<List<String>> triosPlatinum = Arrays.asList(
            Arrays.asList("NA12877", "-", "NA12878")
    );

    @Before
    public void before() throws Exception {
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        sampleIndexDBAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor();
        if (!loaded) {
            load();
            loaded = true;
        }
    }

    public void load() throws Exception {
        clearDB(DB_NAME);
        StudyMetadata.SampleIndexConfigurationVersioned versioned;
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

        versioned = metadataManager.addSampleIndexConfiguration(STUDY_NAME_2, SampleIndexConfiguration.defaultConfiguration()
                .addFileIndexField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.SAMPLE, "DS", new double[]{0, 1, 2})), true);
        assertEquals(2, versioned.getVersion());
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.STAGING, versioned.getStatus());

        runETL(engine, getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        engine.familyIndex(STUDY_NAME_2, trios, new ObjectMap());

        versioned = metadataManager.getStudyMetadata(STUDY_NAME_2).getSampleIndexConfiguration(versioned.getVersion());
        assertEquals(2, versioned.getVersion());
        // Not annotated
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.STAGING, versioned.getStatus());


        // Study 3 - platinum
        metadataManager.addSampleIndexConfiguration(STUDY_NAME_3, SampleIndexConfiguration.defaultConfiguration()
                .addFileIndexField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.FILE, "culprit",
                        IndexFieldConfiguration.Type.CATEGORICAL, "DP", "FS", "MQ", "QD").setNullable(true)), true);

        params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_3)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getPlatinumFile(0), outputUri, params, true, true, true);
        runETL(engine, getPlatinumFile(1), outputUri, params, true, true, true);

        versioned = metadataManager.getStudyMetadata(STUDY_NAME_3).getSampleIndexConfiguration(versioned.getVersion());
        assertEquals(2, versioned.getVersion());
        // Not annotated
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.STAGING, versioned.getStatus());

        // Study 4, dense
        params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME_4)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getResourceUri("variant-test-dense.vcf.gz"), outputUri, params, true, true, true);
        engine.familyIndex(STUDY_NAME_4, trios, new ObjectMap());


        // ---------------- Annotate
        this.variantStorageEngine.annotate(new Query(), new QueryOptions(DefaultVariantAnnotationManager.OUT_DIR, outputUri));
        engine.familyIndex(STUDY_NAME_3, triosPlatinum, new ObjectMap());

        // Study 1 - extra sample index configuration, not staging, only one sample in that configuration

        SampleIndexConfiguration configuration = engine.getMetadataManager().getStudyMetadata(STUDY_NAME).getSampleIndexConfigurationLatest().getConfiguration();
        // Don't modify the configuration.
        versioned = engine.getMetadataManager().addSampleIndexConfiguration(STUDY_NAME, configuration, true);
        assertEquals(2, versioned.getVersion());
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.STAGING, versioned.getStatus());

        engine.sampleIndex(STUDY_NAME, Collections.singletonList("NA19660"), new ObjectMap());
        engine.sampleIndexAnnotate(STUDY_NAME, Collections.singletonList("NA19660"), new ObjectMap());

        versioned = engine.getMetadataManager().getStudyMetadata(STUDY_NAME).getSampleIndexConfigurationLatest(false);
        assertEquals(1, versioned.getVersion());
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE, versioned.getStatus());

        versioned = engine.getMetadataManager().getStudyMetadata(STUDY_NAME).getSampleIndexConfigurationLatest(true);
        assertEquals(2, versioned.getVersion());
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.STAGING, versioned.getStatus());

        engine.getMetadataManager().updateStudyMetadata(STUDY_NAME, sm -> {
            sm.getSampleIndexConfigurationLatest(true).setStatus(StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE);
        });
        versioned = engine.getMetadataManager().getStudyMetadata(STUDY_NAME).getSampleIndexConfigurationLatest(false);
        assertEquals(2, versioned.getVersion());
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE, versioned.getStatus());

        // Study 2 - Latest should be active
        versioned = metadataManager.getStudyMetadata(STUDY_NAME_2).getSampleIndexConfiguration(versioned.getVersion());
        assertEquals(2, versioned.getVersion());
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE, versioned.getStatus());

        // Study 3 - Latest should be active
        versioned = metadataManager.getStudyMetadata(STUDY_NAME_3).getSampleIndexConfiguration(versioned.getVersion());
        assertEquals(2, versioned.getVersion());
        assertEquals(StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE, versioned.getStatus());

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
            Iterator<SampleIndexEntry> indexIt = sampleIndexDBAdaptor.rawIterator(studyId, sample.getId());
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
            // Get the version with ALL samples indexed
            // This is a special case for STUDY, that has a sample index version with missing samples
            int version = sampleIndexDBAdaptor.getSchemaFactory()
                    .getSchema(studyId, dbAdaptor.getMetadataManager().getIndexedSamplesMap(studyId).keySet(), true, false).getVersion();
            String orig = dbAdaptor.getTableNameGenerator().getSampleIndexTableName(studyId, version);
            String copy = orig + "_copy";

            dbAdaptor.getHBaseManager().createTableIfNeeded(copy, Bytes.toBytes(GenomeHelper.COLUMN_FAMILY),
                    Compression.Algorithm.NONE);

            ObjectMap options = new ObjectMap()
                    .append(SampleIndexDriver.SAMPLE_INDEX_VERSION, version)
                    .append(SampleIndexDriver.OUTPUT, copy)
                    .append(SampleIndexDriver.SAMPLES, "all");
            new TestMRExecutor().run(SampleIndexDriver.class, SampleIndexDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    Collections.emptySet(), options));

            new TestMRExecutor().run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    Collections.emptySet(), options));

            if (sampleNames.get(study).containsAll(trios.get(0))) {
                options.put(FamilyIndexDriver.TRIOS, trios.stream().map(trio -> String.join(",", trio)).collect(Collectors.joining(";")));
                options.put(FamilyIndexDriver.OVERWRITE, true);
                new TestMRExecutor().run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
                        dbAdaptor.getArchiveTableName(studyId),
                        dbAdaptor.getVariantTable(),
                        studyId,
                        Collections.emptySet(), options));
            } else if (study.equals(STUDY_NAME_3)) {
                options.put(FamilyIndexDriver.TRIOS, triosPlatinum.stream().map(trio -> String.join(",", trio)).collect(Collectors.joining(";")));
                options.put(FamilyIndexDriver.OVERWRITE, true);
                new TestMRExecutor().run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
                        dbAdaptor.getArchiveTableName(studyId),
                        dbAdaptor.getVariantTable(),
                        studyId,
                        Collections.emptySet(), options));
            }

            Connection c = dbAdaptor.getHBaseManager().getConnection();

            VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), studyId, copy);
            VariantHbaseTestUtils.printSampleIndexTable2(dbAdaptor, Paths.get(newOutputUri()), studyId, copy);

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
        testQueryIndex(new Query(FILE_DATA.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz:culprit=DP"), new Query(STUDY.key(), STUDY_NAME_3).append(GENOTYPE.key(), "NA12877:0/1"));
        testQueryIndex(new Query(FILE_DATA.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz:culprit=DP,QD"),
                new Query(STUDY.key(), STUDY_NAME_3).append(GENOTYPE.key(), "NA12877:1/1,0/1"));

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

        testQueryIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"), new Query(STUDY.key(), STUDY_NAME_3)
                .append(GENOTYPE.key(), "NA12878:0/1;NA12877:0/0,0|0"));
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
        testQueryAnnotationIndex(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=tolerated"));

        //    11:62951221:C:G
        // - SLC22A25 : [missense_variant, stop_lost, 3_prime_UTR_variant, NMD_transcript_variant]
        // - SLC22A10 : [non_coding_transcript_variant, intron_variant]

        // Should return the variant     // 11:62951221:C:G
        testQueryAnnotationIndex(new Query().append(GENE.key(), "SLC22A25").append(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));
        // Should NOT return the variant // 11:62951221:C:G
        testQueryAnnotationIndex(new Query().append(GENE.key(), "SLC22A10").append(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));

        // Should return the variant     // 11:62951221:C:G
        testQueryAnnotationIndex(new Query().append(GENE.key(), "SLC22A25")
                .append(REGION.key(), "2")
                .append(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));


        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained")
                .append(ANNOT_BIOTYPE.key(), "protein_coding")
                .append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));


        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained")
                .append(ANNOT_BIOTYPE.key(), "protein_coding")
                .append(ANNOT_TRANSCRIPT_FLAG.key(), "basic")
                .append(GENE.key(), "BRCA2")
        );
    }

    @Test
    public void testQueryAnnotationIndex_biotype() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_BIOTYPE.key(), PROTEIN_CODING));
        testQueryAnnotationIndex(new Query(ANNOT_BIOTYPE.key(), NONSENSE_MEDIATED_DECAY));
        testQueryAnnotationIndex(new Query(ANNOT_BIOTYPE.key(), PROTEIN_CODING + "," + NONSENSE_MEDIATED_DECAY));
    }

    @Test
    public void testQueryAnnotationIndex_ct() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained"));
//        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "intergenic_variant"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained,mature_miRNA_variant"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained").append(GENE.key(), "HPS4"));
    }

    @Test
    public void testQueryAnnotationIndex_CtBt() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant").append(ANNOT_BIOTYPE.key(), "nonsense_mediated_decay"));
    }

    @Test
    public void testQueryAnnotationIndex_CtBtTf() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant")
                .append(ANNOT_BIOTYPE.key(), "protein_coding")
                .append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
    }

    @Test
    public void testQueryAnnotationIndex_BtTf() throws Exception {
        testQueryAnnotationIndex(new Query()
                .append(ANNOT_BIOTYPE.key(), "protein_coding")
                .append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
    }

    @Test
    public void testQueryAnnotationIndex_Tf() throws Exception {
        // Available flags in cellbase v4, grch37:
        //  basic
        //  CCDS
        //  cds_start_NF
        //  cds_end_NF
        //  mRNA_start_NF
        //  mRNA_end_NF
        //  seleno

        testQueryAnnotationIndex(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "CCDS"));
        testQueryAnnotationIndex(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));

        // Test filtering by other non-covered flags:
        testQueryAnnotationIndex(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "seleno"));
        testQueryAnnotationIndex(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "basic,seleno"));
        testQueryAnnotationIndex(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "CCDS,seleno"));
    }

    @Test
    public void testQueryAnnotationIndex_CtTf() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained")
                .append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained")
                .append(ANNOT_TRANSCRIPT_FLAG.key(), "CCDS"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained")
                .append(ANNOT_TRANSCRIPT_FLAG.key(), "basic,CCDS"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
    }

    @Test
    public void testQueryAnnotationIndex_CtTf_better_than_dbadaptor() throws Exception {
        // These are queries not fully covered using the DBAdaptor (returns more values than expected)
        // SampleIndex is more accurate than DBAdaptor, but still not 100% accurate
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "seleno"));
    }

    @Test
    public void testQueryAnnotationIndex_pop_freq() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL=0"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL>0"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL<0.001"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL>0.005"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL>0.008"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL>=0.005;GNOMAD_GENOMES:ALL>=0.005"));

        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL>=0.005,GNOMAD_GENOMES:ALL>=0.005"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL<0.005,GNOMAD_GENOMES:ALL<0.005"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL>0.005,GNOMAD_GENOMES:ALL>0.005"));
    }

    @Test
    public void testQueryAnnotationIndex_clinical() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "pathogenic"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "likely_benign"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "pathogenic,likely_benign"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "benign"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL.key(), "clinvar"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL.key(), "clinvar,cosmic"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL.key(), "clinvar;cosmic"));
    }

    public void testQueryAnnotationIndex(Query annotationQuery) throws Exception {
        for (String study : studies) {
            for (String sampleName : sampleNames.get(study)) {
                SampleIndexQuery sampleIndexQuery = testQueryIndex(annotationQuery, new Query()
                        .append(STUDY.key(), study)
                        .append(SAMPLE.key(), sampleName));
                assertTrue(!sampleIndexQuery.emptyAnnotationIndex() || !sampleIndexQuery.getAnnotationIndexQuery().getPopulationFrequencyFilter().isNoOp());
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
        queryResult = variantStorageEngine.get(new Query(query), new QueryOptions(QueryOptions.COUNT, true).append(QueryOptions.LIMIT, 5000));
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
        for (Map.Entry<String, Values<SampleFileIndexQuery>> entry : indexQuery.getSampleFileIndexQueryMap().entrySet()) {
            String sample = entry.getKey();
            System.out.println("sample             = " + sample);
            for (SampleFileIndexQuery fileIndexQuery : entry.getValue()) {
                if (entry.getValue().getOperation() == null) {
                    System.out.println("                   = " + fileIndexQuery.getFilters());
                } else {
                    System.out.println("             " + entry.getValue().getOperation() + "    = " + fileIndexQuery.getFilters());
                }
            }
        }
        System.out.println("biotype            = " + indexQuery.getAnnotationIndexQuery().getBiotypeFilter());
        System.out.println("ct                 = " + indexQuery.getAnnotationIndexQuery().getConsequenceTypeFilter());
        System.out.println("transcriptFlag     = " + indexQuery.getAnnotationIndexQuery().getTranscriptFlagFilter());
        System.out.println("ctBtTf             = " + indexQuery.getAnnotationIndexQuery().getCtBtTfFilter());
        System.out.println("clinical           = " + indexQuery.getAnnotationIndexQuery().getClinicalFilter());
        System.out.println("popFreq            = " + indexQuery.getAnnotationIndexQuery().getPopulationFrequencyFilter());
        for (String sample : indexQuery.getSamplesMap().keySet()) {
            if (indexQuery.forSample(sample).hasFatherFilter()) {
                System.out.println("FatherFilter       = " + IndexUtils.parentFilterToString(indexQuery.forSample(sample).getFatherFilter()));
            }
            if (indexQuery.forSample(sample).hasMotherFilter()) {
                System.out.println("MotherFilter       = " + IndexUtils.parentFilterToString(indexQuery.forSample(sample).getMotherFilter()));
            }
//            System.out.println("fileIndex("+sample+") = " + IndexUtils.maskToString(indexQuery.getFileIndexMask(sample), indexQuery.getFileIndex(sample)));
        }
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

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOSS_OF_FUNCTION_SET)));
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(SampleIndexSchema.CUSTOM_LOFE))).append(TYPE.key(), VariantType.INDEL);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(SampleIndexSchema.CUSTOM_LOFE).subList(2, 4)));
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(SampleIndexSchema.CUSTOM_LOFE).subList(2, 4)))
                .append(ANNOT_BIOTYPE.key(), VariantAnnotationConstants.PROTEIN_CODING);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(SampleIndexSchema.CUSTOM_LOFE).subList(2, 4)) + "," + THREE_PRIME_UTR_VARIANT)
                .append(ANNOT_BIOTYPE.key(), VariantAnnotationConstants.PROTEIN_CODING);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());
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
                        new Region("8", 145_100_000, 146_100_000))),
                new Query(ID.key(), "1:101704674:T:C,1:107979396:A:C,7:30915262:C:T,7:31009576:G:T")
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
    public void testAggregationCorrectnessFilterTranscript() throws Exception {
        SampleIndexVariantAggregationExecutor executor = new SampleIndexVariantAggregationExecutor(metadataManager, sampleIndexDBAdaptor);

        String ct = "missense_variant";
        String flag = "basic";

        Query query = new Query(STUDY.key(), STUDY_NAME_3)
                .append(SAMPLE.key(), "NA12877")
                .append(ANNOT_CONSEQUENCE_TYPE.key(), ct)
                .append(ANNOT_TRANSCRIPT_FLAG.key(), flag);
        assertTrue(executor.canUseThisExecutor(query, new QueryOptions(QueryOptions.FACET, "consequenceType>>transcriptFlag")));

        AtomicInteger count = new AtomicInteger(0);
        sampleIndexDBAdaptor.iterator(new Query(query), new QueryOptions()).forEachRemaining(v -> count.incrementAndGet());
        FacetField facet = executor.aggregation(query, new QueryOptions(QueryOptions.FACET, "consequenceType>>transcriptFlag")
                .append("filterTranscript", true)).first();
        FacetField facetAll = executor.aggregation(query, new QueryOptions(QueryOptions.FACET, "consequenceType>>transcriptFlag")
                .append("filterTranscript", false)).first();

        String msg = "aggregation for ct:" + ct + " expected count " + count.get() + " : "
                + JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facet);
        assertEquals(msg, count.get(), facet.getCount());
        FacetField.Bucket bucket = facet.getBuckets().stream().filter(b -> b.getValue().equals(ct)).findFirst().orElse(null);
        FacetField.Bucket bucketAll = facetAll.getBuckets().stream().filter(b -> b.getValue().equals(ct)).findFirst().orElse(null);
        System.out.println("ct = " + ct + " : " + count.get());
        System.out.println("facet    = " + JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facet));
        System.out.println("facetAll = " + JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facetAll));
        if (count.get() == 0) {
            // Count be null if no counts
            if (bucket != null) {
                assertEquals(msg, 0, bucket.getCount());
            }
        } else {
            assertNotNull(msg, bucket);
            assertEquals(msg, count.get(), bucket.getCount());
            // There should be only one filter
            assertEquals(1, facet.getBuckets().size());
        }
    }

    @Test
    public void testAggregationCorrectnessCt() throws Exception {
        SampleIndexSchema schema = sampleIndexDBAdaptor.getSchemaLatest(STUDY_NAME_3);

        CategoricalMultiValuedIndexField<String> field = schema.getCtIndex().getField();
        IndexFieldConfiguration ctConf = field.getConfiguration();
        List<String> cts = new ArrayList<>();
        for (String ct : ctConf.getValues()) {
            if (!field.ambiguous(Collections.singletonList(ct))) {
                cts.add(ct);
            }
        }
        assertNotEquals(new ArrayList<>(), cts);
        cts.remove(TF_BINDING_SITE_VARIANT);
        cts.remove(REGULATORY_REGION_VARIANT);
        System.out.println("cts = " + cts);

        for (String ct : cts) {
            testAggregationCorrectness(ct);
        }
    }

    @Test
    public void testAggregationCorrectnessTFBS() throws Exception {
        testAggregationCorrectness(TF_BINDING_SITE_VARIANT);
    }

    @Test
    public void testAggregationCorrectnessRegulatoryRegionVariant() throws Exception {
        testAggregationCorrectness(REGULATORY_REGION_VARIANT);
    }

    private void testAggregationCorrectness(String ct) throws Exception {
        SampleIndexVariantAggregationExecutor executor = new SampleIndexVariantAggregationExecutor(metadataManager, sampleIndexDBAdaptor);

        Query query = new Query(STUDY.key(), STUDY_NAME_3)
                .append(SAMPLE.key(), "NA12877")
                .append(ANNOT_CONSEQUENCE_TYPE.key(), ct);
        assertTrue(executor.canUseThisExecutor(query, new QueryOptions(QueryOptions.FACET, "consequenceType")));

        AtomicInteger count = new AtomicInteger(0);
        sampleIndexDBAdaptor.iterator(new Query(query), new QueryOptions()).forEachRemaining(v -> count.incrementAndGet());
        FacetField facet = executor.aggregation(query, new QueryOptions(QueryOptions.FACET, "consequenceType")).first();

        assertEquals(count.get(), facet.getCount());
        FacetField.Bucket bucket = facet.getBuckets().stream().filter(b -> b.getValue().equals(ct)).findFirst().orElse(null);
        System.out.println("ct = " + ct + " : " + count.get());
//            System.out.println("facet = " + JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facet));
        String msg = "aggregation for ct:" + ct + " expected count " + count.get() + " : "
                + JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(facet);
        if (count.get() == 0) {
            // Count be null if no counts
            if (bucket != null) {
                assertEquals(msg, 0, bucket.getCount());
            }
        } else {
            assertNotNull(msg, bucket);
            assertEquals(msg, count.get(), bucket.getCount());
        }
    }

    @Test
    public void testAggregation() throws Exception {
        SampleIndexVariantAggregationExecutor executor = new SampleIndexVariantAggregationExecutor(metadataManager, sampleIndexDBAdaptor);

        testAggregation(executor, "qual", STUDY_NAME_3, "NA12877");
        testAggregation(executor, "dp", STUDY_NAME_3, "NA12877");
        testAggregation(executor, "sample:DP", STUDY_NAME_3, "NA12877");
        testAggregation(executor, "qual>>type", STUDY_NAME_3, "NA12877");
        testAggregation(executor, "type>>qual", STUDY_NAME_3, "NA12877");

        testAggregation(executor, "chromosome>>type>>ct");
        testAggregation(executor, "type>>ct");
        testAggregation(executor, "type;gt>>ct");
        testAggregation(executor, "gt>>type>>ct>>biotype");
        testAggregation(executor, "clinicalSignificance>>gt>>type>>ct>>biotype");


        ObjectWriter writer = JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter();

        Query query = new Query(STUDY.key(), STUDY_NAME_3).append(SAMPLE.key(), "NA12877").append(SAMPLE_DATA.key(), "NA12877:DP>=10");
        assertTrue(executor.canUseThisExecutor(query, new QueryOptions(QueryOptions.FACET, "chromosome")));
        System.out.println(writer.writeValueAsString(executor.aggregation(query, new QueryOptions(QueryOptions.FACET, "chromosome")).first()));

        // DP>=11 is not supported
        query = new Query(STUDY.key(), STUDY_NAME_3).append(SAMPLE.key(), "NA12877").append(SAMPLE_DATA.key(), "NA12877:DP>=11");
        assertFalse(executor.canUseThisExecutor(query, new QueryOptions(QueryOptions.FACET, "chromosome")));
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
        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        for (String study : studies) {
            for (String sample : sampleNames.get(study)) {
                DataResult<SampleVariantStats> result = variantStorageEngine.sampleStatsQuery(study, sample, null, null);
                System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result.first()));
            }
        }
    }

    @Test
    public void testSampleVariantStatsFail() throws Exception {
        thrown.expectMessage("No VariantAggregationExecutor found to run the query");
        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        for (String study : studies) {
            for (String sample : sampleNames.get(study)) {
                DataResult<SampleVariantStats> result = variantStorageEngine.sampleStatsQuery(study, sample, new Query()
                        .append(ANNOT_CONSEQUENCE_TYPE.key(), NON_CODING_TRANSCRIPT_EXON_VARIANT), null);
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
                        .append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1000G:ALL<0.9"),
                new QueryOptions()
                        .append(QueryOptions.LIMIT, 10)
                        .append(VariantStorageOptions.APPROXIMATE_COUNT_SAMPLING_SIZE.key(), 200)
                        .append(QueryOptions.COUNT, true));

        assertTrue(result.getApproximateCount());
        assertThat(result.getApproximateCountSamplingSize(), gte(200));
        assertEquals("hadoop + sample_index_table", result.getSource());
    }

}