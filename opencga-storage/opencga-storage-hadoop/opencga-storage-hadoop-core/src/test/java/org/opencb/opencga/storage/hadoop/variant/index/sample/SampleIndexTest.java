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
import org.opencb.biodata.models.variant.avro.ConsequenceType;
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
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.Values;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexOnlyVariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexVariantAggregationExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexVariantQueryExecutor;
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

    @Test
    public void testFamilyIndexQueryCount() {
        List<String> trio = trios.get(0);
        String proband = trio.get(2);
        VariantQueryResult<Variant> result = variantStorageEngine.get(
                new Query()
                        .append(STUDY.key(), STUDY_NAME)
                        .append(SAMPLE.key(), proband + ":compoundheterozygous")
                        .append(INCLUDE_SAMPLE_ID.key(), "true")
                        .append(INCLUDE_SAMPLE.key(), trio)
//                        .append(GENE.key(), "BRCA2")
//                        .append(REGION.key(), "1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22")
                        .append(ANNOT_BIOTYPE.key(), "protein_coding"),
                new QueryOptions()
                        .append(QueryOptions.LIMIT, 10)
                        .append(QueryOptions.COUNT, true));

        System.out.println(result.getResults().stream().map(Variant::getAnnotation).flatMap(v -> v.getConsequenceTypes().stream()).map(ConsequenceType::getGeneName).collect(Collectors.toSet()));

        result = variantStorageEngine.get(
                new Query()
                        .append(STUDY.key(), STUDY_NAME)
                        .append(VariantQueryUtils.SAMPLE_COMPOUND_HETEROZYGOUS.key(), proband)
                        .append(INCLUDE_SAMPLE_ID.key(), "true")
                        .append(INCLUDE_SAMPLE.key(), trio)
                        .append(GENE.key(), "PANK4,MADCAM1")
                        .append(ANNOT_BIOTYPE.key(), "protein_coding"),
                new QueryOptions()
                        .append(QueryOptions.LIMIT, 10)
                        .append(QueryOptions.COUNT, true));

        System.out.println(result.getResults().stream().map(Variant::getAnnotation).flatMap(v -> v.getConsequenceTypes().stream()).map(ConsequenceType::getGeneName).collect(Collectors.toSet()));
    }

    @Test
    public void testSampleIndexOnlyVariantQueryExecutor() {
        testSampleIndexOnlyVariantQueryExecutor(new VariantQuery()
                        .study(STUDY_NAME_1)
                        .sample("NA19660")
                        .includeSampleId(true)
                        .includeGenotype(true), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(VariantField.ID, VariantField.STUDIES_SAMPLES)),
                SampleIndexOnlyVariantQueryExecutor.class);

        testSampleIndexOnlyVariantQueryExecutor(new VariantQuery()
                        .study(STUDY_NAME_1)
                        .sample("NA19660")
                        .biotype("protein_coding")
                        .ct("missense_variant")
                        .includeSampleId(true)
                        .includeGenotype(true), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(VariantField.ID, VariantField.STUDIES_SAMPLES)),
                SampleIndexOnlyVariantQueryExecutor.class);

        testSampleIndexOnlyVariantQueryExecutor(new VariantQuery()
                .study(STUDY_NAME_1)
                .sample("NA19660")
                .includeGenotype(true), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(VariantField.ID)),
                SampleIndexOnlyVariantQueryExecutor.class);

        testSampleIndexOnlyVariantQueryExecutor(
                new VariantQuery()
                        .study(STUDY_NAME_1)
                        .sample("NA19660", "NA19661")
                        .includeGenotype(true),
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(VariantField.ID, VariantField.STUDIES_SAMPLES)),
                SampleIndexVariantQueryExecutor.class);

        testSampleIndexOnlyVariantQueryExecutor(
                new VariantQuery()
                        .study(STUDY_NAME_1)
                        .sample("NA19660", "NA19661"),
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(VariantField.ID)),
                SampleIndexOnlyVariantQueryExecutor.class);
    }

    private void testSampleIndexOnlyVariantQueryExecutor(VariantQuery query, QueryOptions options, Class<?> expected) {
        VariantQueryExecutor variantQueryExecutor = variantStorageEngine.getVariantQueryExecutor(
                query,
                options);
        assertEquals(expected, variantQueryExecutor.getClass());

        ParsedVariantQuery variantQuery = variantStorageEngine.parseQuery(query, options);

        List<Variant> expectedVariants = new ArrayList<>(1000);
        dbAdaptor.iterator(variantQuery, new QueryOptions(options))
                .forEachRemaining(expectedVariants::add);

        List<Variant> actualVariants = new ArrayList<>(1000);
        variantQueryExecutor.iterator(variantQuery.getQuery(), options)
                .forEachRemaining(actualVariants::add);

        long count = variantQueryExecutor.get(variantQuery.getQuery(), new QueryOptions(options)
                .append(QueryOptions.LIMIT, 10)
                .append(QueryOptions.COUNT, true)).getNumMatches();

        expectedVariants.sort(Comparator.comparing(Variant::toString));
        actualVariants.sort(Comparator.comparing(Variant::toString));

        assertEquals(
                expectedVariants.stream().map(String::valueOf).collect(toList()),
                actualVariants.stream().map(String::valueOf).collect(toList()));
        System.out.println("DBAdaptor " + expectedVariants.get(0).toJson());
        System.out.println("Actual    " + actualVariants.get(0).toJson());
        assertEquals(expectedVariants, actualVariants);
        assertEquals(count, actualVariants.size());
    }
}

/*

10:101166544:C:T, 10:101473218:A:G, 10:104229785:C:T, 10:105194086:C:T, 10:11805354:A:G, 10:119043554:C:T, 10:121086097:A:T, 10:123976285:G:A, 10:129899922:T:C, 10:134013974:C:A, 10:135000148:T:C, 10:14941654:C:T, 10:21104613:T:C, 10:26463043:A:T, 10:27964470:G:A, 10:28345418:T:C, 10:33137551:T:C, 10:45953767:A:G, 10:46965018:C:G, 10:49659637:T:C, 10:5435918:G:A, 10:6063508:G:A, 10:61665886:C:A, 10:64573771:C:T, 10:69926319:C:A, 10:71027231:G:C, 10:73044580:G:A, 10:75871735:C:G, 10:7601810:TTTTG:-, 10:81065938:C:T, 10:85997105:G:T, 10:88696622:C:G, 10:88702350:G:C, 10:88730312:T:C, 10:92456132:T:C, 10:93841227:A:G, 10:95552653:T:C, 10:96163039:C:G, 10:99006083:G:A, 10:99130282:T:G, 11:103229027:T:C, 11:1087972:C:T, 11:111324266:T:A, 11:1158073:T:C, 11:117376413:G:A, 11:118529069:G:C, 11:121491782:G:A, 11:123777498:C:T, 11:124015994:C:T, 11:125707761:A:C, 11:1272559:C:T, 11:128781978:T:G, 11:134158745:A:G, 11:1502097:A:G, 11:18210580:C:T, 11:1858262:C:T, 11:233067:C:T, 11:284299:A:G, 11:2869188:C:T, 11:31703352:C:T, 11:36458997:A:G, 11:46745003:C:T, 11:4730985:C:T, 11:47469439:A:G, 11:4791111:T:C, 11:48285982:CTT:-, 11:4976554:A:G, 11:5079946:C:T, 11:51516000:C:T, 11:5248243:A:G, 11:5510598:A:T, 11:5510626:T:C, 11:551753:G:A, 11:55340379:T:C, 11:57982832:T:A, 11:5809548:G:A, 11:6007899:C:T, 11:60285575:A:G, 11:60698054:G:A, 11:60701987:G:A, 11:62010863:C:T, 11:6243804:A:G, 11:62847453:T:C, 11:62951221:C:G, 11:63991581:G:A, 11:64367862:T:C, 11:64591972:T:C, 11:64808682:C:-, 11:67258391:A:G, 11:6789929:C:A, 11:68030173:C:A, 11:68703959:A:G, 11:70279766:C:T, 11:7324475:T:C, 11:73681135:G:A, 11:77635882:A:C, 11:77920930:A:G, 11:870446:G:A, 11:87908448:A:G, 11:89224131:-:TC, 11:92088177:C:T, 11:93517874:C:G, 12:10206925:A:G, 12:104709559:C:T, 12:105568176:G:A, 12:10588530:C:G, 12:10958658:T:C, 12:109696838:G:A, 12:109937534:G:A, 12:110893682:C:A, 12:112580071:C:T, 12:113357209:G:A, 12:11420941:G:T, 12:114377885:G:C, 12:117693817:G:A, 12:118199286:C:G, 12:11905443:G:T, 12:122674758:G:A, 12:123345736:C:G, 12:123799974:A:G, 12:124325977:T:G, 12:124417889:C:T, 12:124968359:C:T, 12:129189702:C:G, 12:129566340:G:A, 12:13214537:A:G, 12:133202004:C:T, 12:133331459:G:C, 12:18443809:C:A, 12:19506870:C:G, 12:26217567:T:C, 12:27077409:G:A, 12:49390677:T:C, 12:50189807:C:A, 12:50190653:C:T, 12:50744119:G:A, 12:51237816:G:A, 12:52885350:T:C, 12:52886911:T:C, 12:52981512:G:A, 12:53217701:C:A, 12:55808469:C:G, 12:55820121:C:T, 12:56335107:A:G, 12:57109931:A:T, 12:57619362:G:A, 12:6091000:A:G, 12:63083521:G:A, 12:6424188:T:C, 12:7981462:T:A, 12:9083336:A:G, 12:93147907:A:T, 12:94976084:T:C, 12:97254620:G:A, 13:110436232:G:A, 13:111319754:T:C, 13:111368164:T:G, 13:113979969:-:CACA, 13:19999913:G:A, 13:23907677:C:T, 13:24243200:T:C, 13:25265139:T:C, 13:26043182:A:C, 13:28009031:G:C, 13:31495179:G:A, 13:31729729:A:G, 13:33590851:T:C, 13:47243196:C:G, 13:52660472:G:A, 13:52951802:T:C, 13:64417500:C:G, 13:95726541:A:G, 13:95863008:C:A, 14:100625902:C:T, 14:101200645:T:C, 14:101350721:T:C, 14:103986255:C:T, 14:105187469:G:A, 14:105344823:G:A, 14:105414252:C:T, 14:105419234:T:C, 14:20528362:A:G, 14:20586432:C:T, 14:21467913:T:G, 14:21511497:C:T, 14:22133997:A:G, 14:39648629:C:T, 14:51057727:G:A, 14:51368610:A:G, 14:52186966:G:A, 14:60574539:A:G, 14:69704553:G:T, 14:75513883:T:C, 14:77942316:G:A, 14:88477882:A:C, 14:90398907:G:A, 14:92088016:G:A, 14:95903306:GTA:-, 14:97002317:G:A, 15:100269796:A:G, 15:29415698:A:G, 15:34673722:C:T, 15:40914177:T:C, 15:41148199:C:T, 15:41149161:G:C, 15:41991315:A:T, 15:42139642:C:T, 15:51217361:T:C, 15:52353498:C:G, 15:52901977:G:A, 15:56959028:C:T, 15:63340647:A:G, 15:63433766:G:A, 15:65715171:G:A, 15:68596203:C:A, 15:68624290:G:A, 15:73994847:C:A, 15:78632830:C:G, 15:79058968:T:C, 15:80215597:G:T, 15:82443939:G:A, 15:84255758:T:C, 15:90126121:C:T, 15:90168693:T:A, 15:90226947:C:A, 15:90628591:G:A, 16:1291250:C:G, 16:16278863:G:T, 16:18872050:C:T, 16:19509305:C:G, 16:1961674:G:C, 16:20376755:T:C, 16:20648702:G:A, 16:23546561:G:C, 16:2815237:A:C, 16:29814234:G:A, 16:3085335:G:C, 16:31004169:T:C, 16:31091209:T:C, 16:3199713:C:T, 16:3297181:C:T, 16:33961918:G:T, 16:3639139:A:G, 16:427820:C:T, 16:4432029:A:C, 16:4938160:T:G, 16:50342658:C:T, 16:51173559:G:A, 16:5140541:G:C, 16:53636000:G:A, 16:68598007:A:G, 16:68712730:C:A, 16:69364437:G:A, 16:70161263:T:C, 16:71660310:G:A, 16:72110541:G:A, 16:76311603:-:T, 16:81211548:G:A, 16:83984844:A:C, 16:84229559:T:C, 16:84229580:C:T, 16:84516309:G:A, 16:84691433:C:T, 16:8738579:A:G, 16:88713236:A:G, 16:88724347:G:T, 16:88805183:G:A, 16:89590168:-:TA, 17:10544416:G:T, 17:11523082:A:G, 17:15341183:A:C, 17:17696531:G:C, 17:21203964:C:G, 17:21318770:G:A, 17:29161358:C:T, 17:30469423:C:A, 17:34328461:A:G, 17:3594277:G:-, 17:3628362:T:C, 17:36478450:G:T, 17:36963226:G:T, 17:38122686:G:A, 17:38955961:G:A, 17:3909383:G:C, 17:39135207:A:G, 17:39334133:T:C, 17:3947533:G:A, 17:39633349:G:C, 17:39661689:G:A, 17:39983849:G:C, 17:41891589:G:A, 17:4463713:A:G, 17:47210506:C:A, 17:47572518:C:T, 17:48452776:A:C, 17:4926882:A:G, 17:56232675:G:A, 17:56598439:T:C, 17:60503892:A:G, 17:6157:A:G, 17:62019103:G:T, 17:65104743:G:A, 17:6515454:C:T, 17:66416357:C:T, 17:6943266:G:A, 17:71390366:C:A, 17:72346855:T:C, 17:7293715:C:T, 17:73949555:C:T, 17:74287204:C:G, 17:74468111:G:A, 17:76230729:T:C, 17:76462770:G:A, 17:7681412:C:G, 17:79477830:C:T, 17:79478019:G:A, 17:8021608:G:C, 17:80391684:A:G, 17:8243661:G:A, 17:8416901:C:T, 18:11609978:C:T, 18:13056682:G:A, 18:13069782:C:T, 18:14752957:G:A, 18:166819:T:C, 18:21100240:C:T, 18:2707619:G:A, 18:28710615:C:A, 18:3164385:C:T, 18:42529996:G:C, 18:56205262:A:C, 18:60191428:G:A, 18:60237388:A:G, 18:61390361:T:C, 18:76753768:C:G, 18:77171061:T:G, 18:77894844:G:A, 18:8784612:A:G, 18:9887205:A:G, 19:10106787:G:A, 19:10221642:A:G, 19:1032689:G:T, 19:10450285:G:A, 19:1047002:A:G, 19:11465316:G:C, 19:12936617:C:G, 19:14495661:A:C, 19:14512489:G:A, 19:14580328:A:G, 19:14589378:C:T, 19:14817548:T:A, 19:14877102:C:T, 19:14910321:A:G, 19:14952017:T:A, 19:15905661:C:A, 19:17450016:T:C, 19:17648350:T:C, 19:18054110:G:A, 19:1811547:A:G, 19:18337260:G:A, 19:18562438:C:T, 19:19168542:A:G, 19:19655670:C:T, 19:1969882:G:T, 19:20229486:G:A, 19:20807047:A:G, 19:21477431:T:C, 19:21606429:G:T, 19:30199200:C:T, 19:33110204:T:C, 19:33792748:G:A, 19:35719106:A:G, 19:36940760:G:T, 19:36981137:A:-, 19:37488499:T:C, 19:39923952:G:A, 19:41018832:A:G, 19:41133643:C:T, 19:42085873:A:G, 19:42301763:A:C, 19:4251069:T:C, 19:4322990:G:A, 19:43243238:A:G, 19:4333711:C:T, 19:43585111:C:G, 19:43585325:G:A, 19:43922060:C:T, 19:43983740:T:C, 19:44470420:T:C, 19:44740602:G:A, 19:44934489:G:A, 19:4511278:C:T, 19:48622427:A:G, 19:4910889:G:T, 19:49513273:C:T, 19:49526191:G:A, 19:49869051:T:G, 19:49950298:C:T, 19:49954803:C:T, 19:501725:G:A, 19:501900:C:A, 19:50312653:C:T, 19:51330932:G:T, 19:51850290:G:A, 19:52004792:-:C, 19:52090100:T:C, 19:5212482:G:A, 19:52887247:-:CAA, 19:52942445:C:T, 19:53995004:G:A, 19:5455976:C:T, 19:54599222:G:T, 19:54677759:T:C, 19:55045042:G:A, 19:55526345:T:G, 19:55871019:A:G, 19:55993876:T:C, 19:56114237:G:A, 19:57036012:G:T, 19:57840547:A:G, 19:57931303:C:A, 19:58002964:G:C, 19:58565233:G:A, 19:58904396:G:A, 19:6731057:T:C, 19:7528734:A:G, 19:7571030:T:A, 19:7755056:G:A, 19:8564288:C:T, 19:8576670:C:T, 19:8645786:A:C, 19:9024994:C:T, 19:9065632:T:C, 19:9072742:T:C, 1:100203648:C:T, 1:100515497:T:C, 1:101704674:T:C, 1:104076462:C:T, 1:104116413:T:C, 1:107979396:A:C, 1:109735416:A:C, 1:116534852:C:T, 1:117487710:A:G, 1:11839971:C:T, 1:11848068:G:C, 1:120611554:T:C, 1:120611715:A:G, 1:13910417:C:T, 1:144917841:T:C, 1:146758054:G:A, 1:150526044:G:C, 1:150970577:G:T, 1:152079989:T:G, 1:152185864:G:A, 1:152192825:C:T, 1:152278689:C:A, 1:152325732:G:A, 1:152770533:G:A, 1:156760887:C:A, 1:158735691:T:C, 1:158813081:T:C, 1:15909850:C:G, 1:160648875:C:T, 1:16386447:G:C, 1:16451767:G:A, 1:1650807:T:C, 1:165389129:G:A, 1:167097739:C:A, 1:169391154:A:G, 1:17586134:A:G, 1:176992553:A:G, 1:17739586:G:A, 1:177902388:G:A, 1:179533915:G:A, 1:182850483:C:T, 1:183184616:C:T, 1:1849529:A:G, 1:18808526:A:C, 1:19175846:T:C, 1:19597420:G:T, 1:200880978:C:T, 1:202129826:T:C, 1:204402500:C:T, 1:208391085:G:A, 1:208391254:C:T, 1:209811886:T:G, 1:21050958:C:T, 1:212873074:C:T, 1:214820299:A:G, 1:215799210:A:G, 1:223717496:C:T, 1:223951857:G:A, 1:225266966:C:G, 1:226553676:C:T, 1:227171487:C:T, 1:228496014:G:A, 1:228504670:C:T, 1:228559994:C:T, 1:229622162:A:G, 1:229623338:T:C, 1:23111551:A:T, 1:234745009:A:G, 1:235652513:T:C, 1:237890437:C:T, 1:2441358:T:C, 1:245133550:GC:-, 1:245704130:G:C, 1:2458010:G:C, 1:247902448:G:A, 1:248436611:G:A, 1:248458876:T:-, 1:248487016:C:T, 1:248525060:T:A, 1:248737511:A:G, 1:25291010:A:T, 1:25890189:A:G, 1:27320356:T:C, 1:29189597:C:T, 1:29475648:T:G, 1:32148571:A:G, 1:33161212:T:C, 1:34330067:A:C, 1:40881041:C:G, 1:40980731:G:T, 1:41285553:T:C, 1:43296173:C:T, 1:47133811:T:C, 1:54605320:-:C, 1:55075062:C:T, 1:59125683:C:T, 1:60381646:C:T, 1:6162054:T:C, 1:62603421:C:T, 1:65860687:A:C, 1:67313249:G:A, 1:67558739:C:T, 1:67852335:G:A, 1:75672376:A:G, 1:76198785:G:A, 1:7797503:C:G, 1:7909737:C:T, 1:84880380:T:C, 1:84944989:A:G, 1:89426902:G:A, 1:89847411:C:T, 1:9009406:C:T, 1:90178336:G:A, 1:92445257:C:G, 20:1600524:T:C, 20:23017017:T:C, 20:25255338:C:T, 20:30795819:T:C, 20:31024207:C:T, 20:31044088:C:T, 20:31897554:G:C, 20:3209072:A:C, 20:36979265:T:C, 20:40714479:G:A, 20:4163302:A:G, 20:42815190:G:A, 20:43379268:C:A, 20:44680412:C:T, 20:44996182:A:G, 20:57045667:T:C, 20:57244493:G:A, 20:60293919:C:A, 20:60640306:GCCAGG:-, 20:61167883:G:A, 20:61881296:A:G, 20:62903550:A:G, 20:7980390:C:T, 21:15954528:G:A, 21:28216674:G:C, 21:33717877:G:A, 21:33956579:T:C, 21:34883618:T:C, 21:37420650:G:A, 21:38437917:A:C, 21:40191638:A:G, 21:43547788:T:C, 21:43824123:G:C, 21:45107518:A:G, 21:45959386:G:A, 21:46020527:C:T, 21:46271452:C:T, 21:47831866:G:A, 22:17469026:G:A, 22:18835221:A:G, 22:19753449:A:G, 22:20780030:-:C, 22:20800835:A:G, 22:21044998:C:T, 22:24109774:T:G, 22:24891380:C:T, 22:26862041:G:A, 22:29454778:G:A, 22:32334229:A:C, 22:36591380:A:G, 22:38369976:A:G, 22:38485540:A:G, 22:41548008:A:G, 22:42416056:A:G, 22:44681612:A:G, 22:45685002:A:G, 22:45813687:G:A, 22:50547247:A:G, 22:50962208:T:G, 2:101096960:C:T, 2:101594191:C:T, 2:107073469:T:A, 2:109381927:A:C, 2:112686988:G:A, 2:120199140:A:G, 2:129025758:C:A, 2:130897620:A:G, 2:136594158:G:A, 2:158636910:G:A, 2:159663599:T:C, 2:159954175:C:T, 2:160035207:T:C, 2:160738677:G:A, 2:160968628:A:G, 2:169985338:C:T, 2:170053505:C:T, 2:170218847:C:G, 2:170354138:G:A, 2:171822466:C:T, 2:172725301:A:G, 2:172945107:C:T, 2:174097106:A:G, 2:176945176:C:G, 2:17698678:A:G, 2:178417142:C:T, 2:179464527:T:C, 2:179578704:G:A, 2:185800905:A:T, 2:186673485:C:T, 2:189932831:T:C, 2:197004439:A:G, 2:203686202:T:C, 2:207041933:T:C, 2:216242917:T:A, 2:219602819:G:A, 2:220283277:T:C, 2:225719693:G:A, 2:228776996:T:C, 2:233712227:ACA:-, 2:233750074:C:T, 2:234707460:G:C, 2:238277795:A:G, 2:240982131:A:G, 2:240985099:G:A, 2:241404317:C:T, 2:241451351:G:A, 2:24149439:G:A, 2:24432839:A:G, 2:277003:A:G, 2:32713706:A:T, 2:32822957:G:A, 2:3392295:A:G, 2:38298139:T:C, 2:55096321:T:C, 2:61647901:A:G, 2:71190384:C:T, 2:71221822:A:G, 2:73339708:G:A, 2:75937801:C:T, 2:96626292:C:T, 2:97637905:T:C, 2:98274527:G:C, 2:99778985:T:C, 3:100712249:T:C, 3:100963154:G:A, 3:101283792:C:G, 3:108634973:C:A, 3:108719470:C:G, 3:111981878:T:C, 3:112993367:G:A, 3:121100283:G:A, 3:121263720:C:A, 3:121351338:C:T, 3:121416623:G:C, 3:123452838:G:A, 3:124379817:T:C, 3:125726048:G:C, 3:1262474:T:C, 3:128369596:A:C, 3:130368301:G:A, 3:13421150:C:T, 3:14183188:G:A, 3:146177815:C:T, 3:14755572:A:G, 3:148847613:G:T, 3:151090424:G:A, 3:154002714:A:G, 3:183823576:T:C, 3:183951431:C:T, 3:183975408:G:A, 3:184003317:C:T, 3:18427924:G:T, 3:190578566:A:G, 3:191093310:A:G, 3:194081635:T:C, 3:195510217:A:G, 3:195701388:G:A, 3:195938177:A:G, 3:196046830:A:G, 3:27472936:C:T, 3:38151731:T:C, 3:38798171:C:T, 3:49690199:G:A, 3:51990315:A:G, 3:52544470:A:G, 3:52825585:T:C, 3:56627598:A:G, 3:56716922:T:G, 3:66287056:G:A, 3:97365074:A:G, 3:97983257:C:G, 3:9798773:C:G, 3:98073313:A:G, 4:104004064:T:C, 4:106155185:C:G, 4:107168431:G:C, 4:109010342:G:A, 4:113352397:G:A, 4:123664204:G:A, 4:129043204:C:G, 4:1388583:A:G, 4:154479430:T:C, 4:155256177:A:G, 4:15569018:G:A, 4:164435265:A:C, 4:173852389:C:T, 4:189012728:G:C, 4:3519881:C:T, 4:39094738:G:A, 4:42003671:A:G, 4:4249884:G:T, 4:42895308:G:A, 4:46086060:T:C, 4:56262374:A:G, 4:5785442:G:A, 4:5991384:T:C, 4:5991476:G:A, 4:69095197:T:C, 4:69687987:C:A, 4:70160342:T:C, 4:7043945:G:T, 4:71469604:C:T, 4:75719517:A:C, 4:7717012:G:A, 4:80905990:C:G, 4:83838262:G:T, 4:84376743:A:T, 4:95578588:G:A, 5:10282396:A:G, 5:109181682:A:T, 5:111611076:A:G, 5:121488506:C:G, 5:122425832:G:T, 5:125802027:G:A, 5:127609633:G:A, 5:134782450:T:A, 5:135388663:A:G, 5:136961566:C:A, 5:13701525:T:C, 5:138861078:C:T, 5:140346468:T:A, 5:140531374:T:C, 5:140605162:C:G, 5:140772427:T:G, 5:141335284:G:A, 5:145508340:A:G, 5:148207447:G:C, 5:154271948:G:A, 5:159835658:A:G, 5:16794916:G:A, 5:169454941:C:G, 5:171723739:T:C, 5:175792605:G:C, 5:176863519:G:C, 5:177422908:A:G, 5:180472498:C:T, 5:32087253:A:G, 5:35861068:T:C, 5:40998196:T:C, 5:41158863:G:A, 5:42719239:A:C, 5:476353:C:T, 5:54253615:C:T, 5:54404015:C:T, 5:56526783:G:A, 5:57751443:A:G, 5:57753149:A:G, 5:73076511:C:A, 5:78340257:C:G, 5:82833391:A:G, 5:89943571:G:T, 5:89985882:A:G, 5:9190404:G:A, 5:95234392:A:C, 5:96237326:G:A, 6:107113715:G:A, 6:111696257:G:A, 6:117246719:C:T, 6:129691132:C:G, 6:133035098:G:A, 6:136683828:A:G, 6:146112348:T:C, 6:151669875:A:G, 6:152470752:C:A, 6:152489294:T:C, 6:155141313:C:T, 6:155597147:C:T, 6:160858188:G:A, 6:166720806:G:C, 6:166873010:C:T, 6:167790110:C:T, 6:170485571:T:C, 6:17665479:G:C, 6:26056549:A:G, 6:26104217:T:C, 6:26370605:T:C, 6:27279852:T:C, 6:29080450:G:A, 6:29911064:A:G, 6:30313268:G:A, 6:30893127:G:A, 6:31110391:G:C, 6:31324864:G:A, 6:31378977:G:A, 6:31540784:C:A, 6:31555657:A:G, 6:31839309:C:T, 6:32370908:T:A, 6:32489748:-:CC, 6:32551959:-:TT, 6:32609271:G:C, 6:32632714:G:C, 6:32802938:C:T, 6:32826233:A:G, 6:32974551:G:T, 6:33756532:G:A, 6:36198421:T:C, 6:36446975:G:C, 6:38746176:G:A, 6:4122249:C:A, 6:41773735:G:A, 6:43251912:A:G, 6:47649265:T:A, 6:51483961:T:C, 6:54186147:T:C, 6:56470690:G:A, 6:62390916:T:C, 6:65300143:G:C, 6:656555:G:T, 6:7246998:G:A, 6:72889472:A:G, 6:74354175:C:T, 6:79656570:G:A, 6:82461520:A:G, 6:83949261:T:C, 6:84799059:C:T, 6:90459454:G:A, 6:9900600:-:GAG, 7:100391581:T:C, 7:100807230:G:T, 7:102112980:G:A, 7:104110492:C:T, 7:106524689:C:T, 7:107834734:C:T, 7:117282644:A:G, 7:12417407:C:T, 7:134925411:G:A, 7:138732497:G:A, 7:150696111:T:G, 7:150935430:G:C, 7:154681216:G:A, 7:156742675:C:T, 7:20698270:A:G, 7:20778646:G:A, 7:2645526:G:A, 7:30915262:C:T, 7:31009576:G:T, 7:36366483:G:C, 7:37907304:T:C, 7:43664280:A:G, 7:44620836:C:A, 7:45124465:A:T, 7:47872845:A:G, 7:50435777:T:G, 7:5112057:C:G, 7:5518331:A:G, 7:55433884:A:C, 7:6026988:G:A, 7:63225873:C:T, 7:6550540:G:A, 7:66098384:G:A, 7:66703328:G:A, 7:75659815:T:C, 7:87160618:A:C, 7:91503228:C:T, 7:92098776:C:T, 7:97823125:G:A, 7:99580907:C:G, 8:104337096:A:G, 8:10480268:A:C, 8:110302047:T:G, 8:11996150:C:G, 8:124448804:T:A, 8:124665124:C:T, 8:12878807:T:G, 8:142367400:G:A, 8:142488837:G:A, 8:144332082:T:C, 8:144671685:G:C, 8:144697041:A:G, 8:144946252:C:T, 8:144995736:G:A, 8:144998514:C:T, 8:145693720:A:G, 8:146033347:T:C, 8:146115367:A:G, 8:146156247:C:A, 8:18257854:T:C, 8:2021421:G:T, 8:22864622:T:C, 8:23150878:T:G, 8:27634589:T:C, 8:27925796:A:T, 8:30585310:T:C, 8:30695226:C:T, 8:3200877:C:T, 8:41132742:A:T, 8:48173561:G:A, 8:57026229:C:A, 8:74005131:A:G, 8:74888616:G:C, 8:75157094:C:T, 8:75737733:A:G, 8:8234192:G:C, 8:977600:C:T, 9:103064530:G:A, 9:107361439:G:C, 9:112069477:T:A, 9:113169630:T:C, 9:115968797:C:T, 9:116028559:C:A, 9:117835931:G:A, 9:125637471:A:T, 9:125920376:G:A, 9:126520068:T:C, 9:127220952:T:C, 9:131403096:A:G, 9:131689361:G:A, 9:132382596:C:A, 9:132591509:A:G, 9:133710820:-:C, 9:133761001:A:G, 9:133951230:C:T, 9:135139901:T:C, 9:136340200:T:G, 9:138591266:A:G, 9:139100805:T:C, 9:139273288:C:T, 9:139391636:G:A, 9:139413908:C:T, 9:139650678:A:G, 9:139656670:T:C, 9:139937795:T:C, 9:139990813:C:T, 9:14775859:G:A, 9:17466802:A:G, 9:18681821:A:G, 9:19058483:C:A, 9:27524731:A:G, 9:33935736:A:G, 9:34379692:C:T, 9:35606884:G:A, 9:35870001:T:C, 9:37441650:T:C, 9:429719:T:C, 9:90343780:A:C, 9:91978397:C:T, X:107976940:G:C, X:11316892:C:T, X:117700141:A:G, X:13677862:G:A, X:153151285:T:-, X:2408437:G:A, X:295231:A:G, X:3241791:G:A, X:45051111:C:T, X:48460314:A:G, X:70146475:G:C
1:1650807:T:C, 1:1849529:A:G, 1:2441358:T:C, 1:2458010:G:C, 1:6162054:T:C, 1:7797503:C:G, 1:7909737:C:T, 1:9009406:C:T, 1:11839971:C:T, 1:11848068:G:C, 1:13910417:C:T, 1:15909850:C:G, 1:16386447:G:C, 1:16451767:G:A, 1:17586134:A:G, 1:17739586:G:A, 1:18808526:A:C, 1:19175846:T:C, 1:19597420:G:T, 1:21050958:C:T, 1:23111551:A:T, 1:25291010:A:T, 1:25890189:A:G, 1:27320356:T:C, 1:29189597:C:T, 1:29475648:T:G, 1:32148571:A:G, 1:33161212:T:C, 1:34330067:A:C, 1:40881041:C:G, 1:40980731:G:T, 1:41285553:T:C, 1:43296173:C:T, 1:47133811:T:C, 1:54605320:-:C, 1:55075062:C:T, 1:59125683:C:T, 1:60381646:C:T, 1:62603421:C:T, 1:65860687:A:C, 1:67313249:G:A, 1:67558739:C:T, 1:67852335:G:A, 1:75672376:A:G, 1:76198785:G:A, 1:84880380:T:C, 1:84944989:A:G, 1:89426902:G:A, 1:89847411:C:T, 1:90178336:G:A, 1:92445257:C:G, 1:100203648:C:T, 1:100515497:T:C, 1:101704674:T:C, 1:104076462:C:T, 1:104116413:T:C, 1:107979396:A:C, 1:109735416:A:C, 1:116534852:C:T, 1:117487710:A:G, 1:120611554:T:C, 1:120611715:A:G, 1:144917841:T:C, 1:146758054:G:A, 1:150526044:G:C, 1:150970577:G:T, 1:152079989:T:G, 1:152185864:G:A, 1:152192825:C:T, 1:152278689:C:A, 1:152325732:G:A, 1:152770533:G:A, 1:156760887:C:A, 1:158735691:T:C, 1:158813081:T:C, 1:160648875:C:T, 1:165389129:G:A, 1:167097739:C:A, 1:169391154:A:G, 1:176992553:A:G, 1:177902388:G:A, 1:179533915:G:A, 1:182850483:C:T, 1:183184616:C:T, 1:200880978:C:T, 1:202129826:T:C, 1:204402500:C:T, 1:208391085:G:A, 1:208391254:C:T, 1:209811886:T:G, 1:212873074:C:T, 1:214820299:A:G, 1:215799210:A:G, 1:223717496:C:T, 1:223951857:G:A, 1:225266966:C:G, 1:226553676:C:T, 1:227171487:C:T, 1:228496014:G:A, 1:228504670:C:T, 1:228559994:C:T, 1:229622162:A:G, 1:229623338:T:C, 1:234745009:A:G, 1:235652513:T:C, 1:237890437:C:T, 1:245133550:GC:-, 1:245704130:G:C, 1:247902448:G:A, 1:248436611:G:A, 1:248458876:T:-, 1:248487016:C:T, 1:248525060:T:A, 1:248737511:A:G, 10:5435918:G:A, 10:6063508:G:A, 10:7601810:TTTTG:-, 10:11805354:A:G, 10:14941654:C:T, 10:21104613:T:C, 10:26463043:A:T, 10:27964470:G:A, 10:28345418:T:C, 10:33137551:T:C, 10:45953767:A:G, 10:46965018:C:G, 10:49659637:T:C, 10:61665886:C:A, 10:64573771:C:T, 10:69926319:C:A, 10:71027231:G:C, 10:73044580:G:A, 10:75871735:C:G, 10:81065938:C:T, 10:85997105:G:T, 10:88696622:C:G, 10:88702350:G:C, 10:88730312:T:C, 10:92456132:T:C, 10:93841227:A:G, 10:95552653:T:C, 10:96163039:C:G, 10:99006083:G:A, 10:99130282:T:G, 10:101166544:C:T, 10:101473218:A:G, 10:104229785:C:T, 10:105194086:C:T, 10:119043554:C:T, 10:121086097:A:T, 10:123976285:G:A, 10:129899922:T:C, 10:134013974:C:A, 10:135000148:T:C, 11:233067:C:T, 11:284299:A:G, 11:551753:G:A, 11:870446:G:A, 11:1087972:C:T, 11:1158073:T:C, 11:1272559:C:T, 11:1502097:A:G, 11:1858262:C:T, 11:2869188:C:T, 11:4730985:C:T, 11:4791111:T:C, 11:4976554:A:G, 11:5079946:C:T, 11:5248243:A:G, 11:5510598:A:T, 11:5510626:T:C, 11:5809548:G:A, 11:6007899:C:T, 11:6243804:A:G, 11:6789929:C:A, 11:7324475:T:C, 11:18210580:C:T, 11:31703352:C:T, 11:36458997:A:G, 11:46745003:C:T, 11:47469439:A:G, 11:48285982:CTT:-, 11:51516000:C:T, 11:55340379:T:C, 11:57982832:T:A, 11:60285575:A:G, 11:60698054:G:A, 11:60701987:G:A, 11:62010863:C:T, 11:62847453:T:C, 11:62951221:C:G, 11:63991581:G:A, 11:64367862:T:C, 11:64591972:T:C, 11:64808682:C:-, 11:67258391:A:G, 11:68030173:C:A, 11:68703959:A:G, 11:70279766:C:T, 11:73681135:G:A, 11:77635882:A:C, 11:77920930:A:G, 11:87908448:A:G, 11:89224131:-:TC, 11:92088177:C:T, 11:93517874:C:G, 11:103229027:T:C, 11:111324266:T:A, 11:117376413:G:A, 11:118529069:G:C, 11:121491782:G:A, 11:123777498:C:T, 11:124015994:C:T, 11:125707761:A:C, 11:128781978:T:G, 11:134158745:A:G, 12:6091000:A:G, 12:6424188:T:C, 12:7981462:T:A, 12:9083336:A:G, 12:10206925:A:G, 12:10588530:C:G, 12:10958658:T:C, 12:11420941:G:T, 12:11905443:G:T, 12:13214537:A:G, 12:18443809:C:A, 12:19506870:C:G, 12:26217567:T:C, 12:27077409:G:A, 12:49390677:T:C, 12:50189807:C:A, 12:50190653:C:T, 12:50744119:G:A, 12:51237816:G:A, 12:52885350:T:C, 12:52886911:T:C, 12:52981512:G:A, 12:53217701:C:A, 12:55808469:C:G, 12:55820121:C:T, 12:56335107:A:G, 12:57109931:A:T, 12:57619362:G:A, 12:63083521:G:A, 12:93147907:A:T, 12:94976084:T:C, 12:97254620:G:A, 12:104709559:C:T, 12:105568176:G:A, 12:109696838:G:A, 12:109937534:G:A, 12:110893682:C:A, 12:112580071:C:T, 12:113357209:G:A, 12:114377885:G:C, 12:117693817:G:A, 12:118199286:C:G, 12:122674758:G:A, 12:123345736:C:G, 12:123799974:A:G, 12:124325977:T:G, 12:124417889:C:T, 12:124968359:C:T, 12:129189702:C:G, 12:129566340:G:A, 12:133202004:C:T, 12:133331459:G:C, 13:19999913:G:A, 13:23907677:C:T, 13:24243200:T:C, 13:25265139:T:C, 13:26043182:A:C, 13:28009031:G:C, 13:31495179:G:A, 13:31729729:A:G, 13:33590851:T:C, 13:47243196:C:G, 13:52660472:G:A, 13:52951802:T:C, 13:64417500:C:G, 13:95726541:A:G, 13:95863008:C:A, 13:110436232:G:A, 13:111319754:T:C, 13:111368164:T:G, 13:113979969:-:CACA, 14:20528362:A:G, 14:20586432:C:T, 14:21467913:T:G, 14:21511497:C:T, 14:22133997:A:G, 14:39648629:C:T, 14:51057727:G:A, 14:51368610:A:G, 14:52186966:G:A, 14:60574539:A:G, 14:69704553:G:T, 14:75513883:T:C, 14:77942316:G:A, 14:88477882:A:C, 14:90398907:G:A, 14:92088016:G:A, 14:95903306:GTA:-, 14:97002317:G:A, 14:100625902:C:T, 14:101200645:T:C, 14:101350721:T:C, 14:103986255:C:T, 14:105187469:G:A, 14:105344823:G:A, 14:105414252:C:T, 14:105419234:T:C, 15:29415698:A:G, 15:34673722:C:T, 15:40914177:T:C, 15:41148199:C:T, 15:41149161:G:C, 15:41991315:A:T, 15:42139642:C:T, 15:51217361:T:C, 15:52353498:C:G, 15:52901977:G:A, 15:56959028:C:T, 15:63340647:A:G, 15:63433766:G:A, 15:65715171:G:A, 15:68596203:C:A, 15:68624290:G:A, 15:73994847:C:A, 15:78632830:C:G, 15:79058968:T:C, 15:80215597:G:T, 15:82443939:G:A, 15:84255758:T:C, 15:90126121:C:T, 15:90168693:T:A, 15:90226947:C:A, 15:90628591:G:A, 15:100269796:A:G, 16:427820:C:T, 16:1291250:C:G, 16:1961674:G:C, 16:2815237:A:C, 16:3085335:G:C, 16:3199713:C:T, 16:3297181:C:T, 16:3639139:A:G, 16:4432029:A:C, 16:4938160:T:G, 16:5140541:G:C, 16:8738579:A:G, 16:16278863:G:T, 16:18872050:C:T, 16:19509305:C:G, 16:20376755:T:C, 16:20648702:G:A, 16:23546561:G:C, 16:29814234:G:A, 16:31004169:T:C, 16:31091209:T:C, 16:33961918:G:T, 16:50342658:C:T, 16:51173559:G:A, 16:53636000:G:A, 16:68598007:A:G, 16:68712730:C:A, 16:69364437:G:A, 16:70161263:T:C, 16:71660310:G:A, 16:72110541:G:A, 16:76311603:-:T, 16:81211548:G:A, 16:83984844:A:C, 16:84229559:T:C, 16:84229580:C:T, 16:84516309:G:A, 16:84691433:C:T, 16:88713236:A:G, 16:88724347:G:T, 16:88805183:G:A, 16:89590168:-:TA, 17:6157:A:G, 17:3594277:G:-, 17:3628362:T:C, 17:3909383:G:C, 17:3947533:G:A, 17:4463713:A:G, 17:4926882:A:G, 17:6515454:C:T, 17:6943266:G:A, 17:7293715:C:T, 17:7681412:C:G, 17:8021608:G:C, 17:8243661:G:A, 17:8416901:C:T, 17:10544416:G:T, 17:11523082:A:G, 17:15341183:A:C, 17:17696531:G:C, 17:21203964:C:G, 17:21318770:G:A, 17:29161358:C:T, 17:30469423:C:A, 17:34328461:A:G, 17:36478450:G:T, 17:36963226:G:T, 17:38122686:G:A, 17:38955961:G:A, 17:39135207:A:G, 17:39334133:T:C, 17:39633349:G:C, 17:39661689:G:A, 17:39983849:G:C, 17:41891589:G:A, 17:47210506:C:A, 17:47572518:C:T, 17:48452776:A:C, 17:56232675:G:A, 17:56598439:T:C, 17:60503892:A:G, 17:62019103:G:T, 17:65104743:G:A, 17:66416357:C:T, 17:71390366:C:A, 17:72346855:T:C, 17:73949555:C:T, 17:74287204:C:G, 17:74468111:G:A, 17:76230729:T:C, 17:76462770:G:A, 17:79477830:C:T, 17:79478019:G:A, 17:80391684:A:G, 18:166819:T:C, 18:2707619:G:A, 18:3164385:C:T, 18:8784612:A:G, 18:9887205:A:G, 18:11609978:C:T, 18:13056682:G:A, 18:13069782:C:T, 18:14752957:G:A, 18:21100240:C:T, 18:28710615:C:A, 18:42529996:G:C, 18:56205262:A:C, 18:60191428:G:A, 18:60237388:A:G, 18:61390361:T:C, 18:76753768:C:G, 18:77171061:T:G, 18:77894844:G:A, 19:501725:G:A, 19:501900:C:A, 19:1032689:G:T, 19:1047002:A:G, 19:1811547:A:G, 19:1969882:G:T, 19:4251069:T:C, 19:4322990:G:A, 19:4333711:C:T, 19:4511278:C:T, 19:4910889:G:T, 19:5212482:G:A, 19:5455976:C:T, 19:6731057:T:C, 19:7528734:A:G, 19:7571030:T:A, 19:7755056:G:A, 19:8564288:C:T, 19:8576670:C:T, 19:8645786:A:C, 19:9024994:C:T, 19:9065632:T:C, 19:9072742:T:C, 19:10106787:G:A, 19:10221642:A:G, 19:10450285:G:A, 19:11465316:G:C, 19:12936617:C:G, 19:14495661:A:C, 19:14512489:G:A, 19:14580328:A:G, 19:14589378:C:T, 19:14817548:T:A, 19:14877102:C:T, 19:14910321:A:G, 19:14952017:T:A, 19:15905661:C:A, 19:17450016:T:C, 19:17648350:T:C, 19:18054110:G:A, 19:18337260:G:A, 19:18562438:C:T, 19:19168542:A:G, 19:19655670:C:T, 19:20229486:G:A, 19:20807047:A:G, 19:21477431:T:C, 19:21606429:G:T, 19:30199200:C:T, 19:33110204:T:C, 19:33792748:G:A, 19:35719106:A:G, 19:36940760:G:T, 19:36981137:A:-, 19:37488499:T:C, 19:39923952:G:A, 19:41018832:A:G, 19:41133643:C:T, 19:42085873:A:G, 19:42301763:A:C, 19:43243238:A:G, 19:43585111:C:G, 19:43585325:G:A, 19:43922060:C:T, 19:43983740:T:C, 19:44470420:T:C, 19:44740602:G:A, 19:44934489:G:A, 19:48622427:A:G, 19:49513273:C:T, 19:49526191:G:A, 19:49869051:T:G, 19:49950298:C:T, 19:49954803:C:T, 19:50312653:C:T, 19:51330932:G:T, 19:51850290:G:A, 19:52004792:-:C, 19:52090100:T:C, 19:52887247:-:CAA, 19:52942445:C:T, 19:53995004:G:A, 19:54599222:G:T, 19:54677759:T:C, 19:55045042:G:A, 19:55526345:T:G, 19:55871019:A:G, 19:55993876:T:C, 19:56114237:G:A, 19:57036012:G:T, 19:57840547:A:G, 19:57931303:C:A, 19:58002964:G:C, 19:58565233:G:A, 19:58904396:G:A, 2:277003:A:G, 2:3392295:A:G, 2:17698678:A:G, 2:24149439:G:A, 2:24432839:A:G, 2:32713706:A:T, 2:32822957:G:A, 2:38298139:T:C, 2:55096321:T:C, 2:61647901:A:G, 2:71190384:C:T, 2:71221822:A:G, 2:73339708:G:A, 2:75937801:C:T, 2:96626292:C:T, 2:97637905:T:C, 2:98274527:G:C, 2:99778985:T:C, 2:101096960:C:T, 2:101594191:C:T, 2:107073469:T:A, 2:109381927:A:C, 2:112686988:G:A, 2:120199140:A:G, 2:129025758:C:A, 2:130897620:A:G, 2:136594158:G:A, 2:158636910:G:A, 2:159663599:T:C, 2:159954175:C:T, 2:160035207:T:C, 2:160738677:G:A, 2:160968628:A:G, 2:169985338:C:T, 2:170053505:C:T, 2:170218847:C:G, 2:170354138:G:A, 2:171822466:C:T, 2:172725301:A:G, 2:172945107:C:T, 2:174097106:A:G, 2:176945176:C:G, 2:178417142:C:T, 2:179464527:T:C, 2:179578704:G:A, 2:185800905:A:T, 2:186673485:C:T, 2:189932831:T:C, 2:197004439:A:G, 2:203686202:T:C, 2:207041933:T:C, 2:216242917:T:A, 2:219602819:G:A, 2:220283277:T:C, 2:225719693:G:A, 2:228776996:T:C, 2:233712227:ACA:-, 2:233750074:C:T, 2:234707460:G:C, 2:238277795:A:G, 2:240982131:A:G, 2:240985099:G:A, 2:241404317:C:T, 2:241451351:G:A, 20:1600524:T:C, 20:3209072:A:C, 20:4163302:A:G, 20:7980390:C:T, 20:23017017:T:C, 20:25255338:C:T, 20:30795819:T:C, 20:31024207:C:T, 20:31044088:C:T, 20:31897554:G:C, 20:36979265:T:C, 20:40714479:G:A, 20:42815190:G:A, 20:43379268:C:A, 20:44680412:C:T, 20:44996182:A:G, 20:57045667:T:C, 20:57244493:G:A, 20:60293919:C:A, 20:60640306:GCCAGG:-, 20:61167883:G:A, 20:61881296:A:G, 20:62903550:A:G, 21:15954528:G:A, 21:28216674:G:C, 21:33717877:G:A, 21:33956579:T:C, 21:34883618:T:C, 21:37420650:G:A, 21:38437917:A:C, 21:40191638:A:G, 21:43547788:T:C, 21:43824123:G:C, 21:45107518:A:G, 21:45959386:G:A, 21:46020527:C:T, 21:46271452:C:T, 21:47831866:G:A, 22:17469026:G:A, 22:18835221:A:G, 22:19753449:A:G, 22:20780030:-:C, 22:20800835:A:G, 22:21044998:C:T, 22:24109774:T:G, 22:24891380:C:T, 22:26862041:G:A, 22:29454778:G:A, 22:32334229:A:C, 22:36591380:A:G, 22:38369976:A:G, 22:38485540:A:G, 22:41548008:A:G, 22:42416056:A:G, 22:44681612:A:G, 22:45685002:A:G, 22:45813687:G:A, 22:50547247:A:G, 22:50962208:T:G, 3:1262474:T:C, 3:9798773:C:G, 3:13421150:C:T, 3:14183188:G:A, 3:14755572:A:G, 3:18427924:G:T, 3:27472936:C:T, 3:38151731:T:C, 3:38798171:C:T, 3:49690199:G:A, 3:51990315:A:G, 3:52544470:A:G, 3:52825585:T:C, 3:56627598:A:G, 3:56716922:T:G, 3:66287056:G:A, 3:97365074:A:G, 3:97983257:C:G, 3:98073313:A:G, 3:100712249:T:C, 3:100963154:G:A, 3:101283792:C:G, 3:108634973:C:A, 3:108719470:C:G, 3:111981878:T:C, 3:112993367:G:A, 3:121100283:G:A, 3:121263720:C:A, 3:121351338:C:T, 3:121416623:G:C, 3:123452838:G:A, 3:124379817:T:C, 3:125726048:G:C, 3:128369596:A:C, 3:130368301:G:A, 3:146177815:C:T, 3:148847613:G:T, 3:151090424:G:A, 3:154002714:A:G, 3:183823576:T:C, 3:183951431:C:T, 3:183975408:G:A, 3:184003317:C:T, 3:190578566:A:G, 3:191093310:A:G, 3:194081635:T:C, 3:195510217:A:G, 3:195701388:G:A, 3:195938177:A:G, 3:196046830:A:G, 4:1388583:A:G, 4:3519881:C:T, 4:4249884:G:T, 4:5785442:G:A, 4:5991384:T:C, 4:5991476:G:A, 4:7043945:G:T, 4:7717012:G:A, 4:15569018:G:A, 4:39094738:G:A, 4:42003671:A:G, 4:42895308:G:A, 4:46086060:T:C, 4:56262374:A:G, 4:69095197:T:C, 4:69687987:C:A, 4:70160342:T:C, 4:71469604:C:T, 4:75719517:A:C, 4:80905990:C:G, 4:83838262:G:T, 4:84376743:A:T, 4:95578588:G:A, 4:104004064:T:C, 4:106155185:C:G, 4:107168431:G:C, 4:109010342:G:A, 4:113352397:G:A, 4:123664204:G:A, 4:129043204:C:G, 4:154479430:T:C, 4:155256177:A:G, 4:164435265:A:C, 4:173852389:C:T, 4:189012728:G:C, 5:476353:C:T, 5:9190404:G:A, 5:10282396:A:G, 5:13701525:T:C, 5:16794916:G:A, 5:32087253:A:G, 5:35861068:T:C, 5:40998196:T:C, 5:41158863:G:A, 5:42719239:A:C, 5:54253615:C:T, 5:54404015:C:T, 5:56526783:G:A, 5:57751443:A:G, 5:57753149:A:G, 5:73076511:C:A, 5:78340257:C:G, 5:82833391:A:G, 5:89943571:G:T, 5:89985882:A:G, 5:95234392:A:C, 5:96237326:G:A, 5:109181682:A:T, 5:111611076:A:G, 5:121488506:C:G, 5:122425832:G:T, 5:125802027:G:A, 5:127609633:G:A, 5:134782450:T:A, 5:135388663:A:G, 5:136961566:C:A, 5:138861078:C:T, 5:140346468:T:A, 5:140531374:T:C, 5:140605162:C:G, 5:140772427:T:G, 5:141335284:G:A, 5:145508340:A:G, 5:148207447:G:C, 5:154271948:G:A, 5:159835658:A:G, 5:169454941:C:G, 5:171723739:T:C, 5:175792605:G:C, 5:176863519:G:C, 5:177422908:A:G, 5:180472498:C:T, 6:656555:G:T, 6:4122249:C:A, 6:7246998:G:A, 6:9900600:-:GAG, 6:17665479:G:C, 6:26056549:A:G, 6:26104217:T:C, 6:26370605:T:C, 6:27279852:T:C, 6:29080450:G:A, 6:29911064:A:G, 6:30313268:G:A, 6:30893127:G:A, 6:31110391:G:C, 6:31324864:G:A, 6:31378977:G:A, 6:31540784:C:A, 6:31555657:A:G, 6:31839309:C:T, 6:32370908:T:A, 6:32489748:-:CC, 6:32551959:-:TT, 6:32609271:G:C, 6:32632714:G:C, 6:32802938:C:T, 6:32826233:A:G, 6:32974551:G:T, 6:33756532:G:A, 6:36198421:T:C, 6:36446975:G:C, 6:38746176:G:A, 6:41773735:G:A, 6:43251912:A:G, 6:47649265:T:A, 6:51483961:T:C, 6:54186147:T:C, 6:56470690:G:A, 6:62390916:T:C, 6:65300143:G:C, 6:72889472:A:G, 6:74354175:C:T, 6:79656570:G:A, 6:82461520:A:G, 6:83949261:T:C, 6:84799059:C:T, 6:90459454:G:A, 6:107113715:G:A, 6:111696257:G:A, 6:117246719:C:T, 6:129691132:C:G, 6:133035098:G:A, 6:136683828:A:G, 6:146112348:T:C, 6:151669875:A:G, 6:152470752:C:A, 6:152489294:T:C, 6:155141313:C:T, 6:155597147:C:T, 6:160858188:G:A, 6:166720806:G:C, 6:166873010:C:T, 6:167790110:C:T, 6:170485571:T:C, 7:2645526:G:A, 7:5112057:C:G, 7:5518331:A:G, 7:6026988:G:A, 7:6550540:G:A, 7:12417407:C:T, 7:20698270:A:G, 7:20778646:G:A, 7:30915262:C:T, 7:31009576:G:T, 7:36366483:G:C, 7:37907304:T:C, 7:43664280:A:G, 7:44620836:C:A, 7:45124465:A:T, 7:47872845:A:G, 7:50435777:T:G, 7:55433884:A:C, 7:63225873:C:T, 7:66098384:G:A, 7:66703328:G:A, 7:75659815:T:C, 7:87160618:A:C, 7:91503228:C:T, 7:92098776:C:T, 7:97823125:G:A, 7:99580907:C:G, 7:100391581:T:C, 7:100807230:G:T, 7:102112980:G:A, 7:104110492:C:T, 7:106524689:C:T, 7:107834734:C:T, 7:117282644:A:G, 7:134925411:G:A, 7:138732497:G:A, 7:150696111:T:G, 7:150935430:G:C, 7:154681216:G:A, 7:156742675:C:T, 8:977600:C:T, 8:2021421:G:T, 8:3200877:C:T, 8:8234192:G:C, 8:10480268:A:C, 8:11996150:C:G, 8:12878807:T:G, 8:18257854:T:C, 8:22864622:T:C, 8:23150878:T:G, 8:27634589:T:C, 8:27925796:A:T, 8:30585310:T:C, 8:30695226:C:T, 8:41132742:A:T, 8:48173561:G:A, 8:57026229:C:A, 8:74005131:A:G, 8:74888616:G:C, 8:75157094:C:T, 8:75737733:A:G, 8:104337096:A:G, 8:110302047:T:G, 8:124448804:T:A, 8:124665124:C:T, 8:142367400:G:A, 8:142488837:G:A, 8:144332082:T:C, 8:144671685:G:C, 8:144697041:A:G, 8:144946252:C:T, 8:144995736:G:A, 8:144998514:C:T, 8:145693720:A:G, 8:146033347:T:C, 8:146115367:A:G, 8:146156247:C:A, 9:429719:T:C, 9:14775859:G:A, 9:17466802:A:G, 9:18681821:A:G, 9:19058483:C:A, 9:27524731:A:G, 9:33935736:A:G, 9:34379692:C:T, 9:35606884:G:A, 9:35870001:T:C, 9:37441650:T:C, 9:90343780:A:C, 9:91978397:C:T, 9:103064530:G:A, 9:107361439:G:C, 9:112069477:T:A, 9:113169630:T:C, 9:115968797:C:T, 9:116028559:C:A, 9:117835931:G:A, 9:125637471:A:T, 9:125920376:G:A, 9:126520068:T:C, 9:127220952:T:C, 9:131403096:A:G, 9:131689361:G:A, 9:132382596:C:A, 9:132591509:A:G, 9:133710820:-:C, 9:133761001:A:G, 9:133951230:C:T, 9:135139901:T:C, 9:136340200:T:G, 9:138591266:A:G, 9:139100805:T:C, 9:139273288:C:T, 9:139391636:G:A, 9:139413908:C:T, 9:139650678:A:G, 9:139656670:T:C, 9:139937795:T:C, 9:139990813:C:T, X:295231:A:G, X:2408437:G:A, X:3241791:G:A, X:11316892:C:T, X:13677862:G:A, X:45051111:C:T, X:48460314:A:G, X:70146475:G:C, X:107976940:G:C, X:117700141:A:G, X:153151285:T:-

 */