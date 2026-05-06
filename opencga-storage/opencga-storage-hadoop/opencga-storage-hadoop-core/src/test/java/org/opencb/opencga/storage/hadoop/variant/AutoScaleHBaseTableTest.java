package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.HBaseCompat;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseSampleIndexDBAdaptor;

import static org.junit.Assert.assertEquals;

@Category(LongTests.class)
public class AutoScaleHBaseTableTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    private VariantHadoopDBAdaptor dbAdaptor;
    private HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor;
    private HadoopVariantStorageEngine engine;

    @Before
    public void before() throws Exception {
        clearDB(DB_NAME);
        engine = getVariantStorageEngine();
        dbAdaptor = engine.getDBAdaptor();
        sampleIndexDBAdaptor = engine.getSampleIndexDBAdaptor();

    }

    @Test
    public void testAutoScaleTables() throws Exception {

        int archiveSplitsPerBatch = 10;
        int samplesPerSplit = 2;
        int extraSplits = 3;

        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.LOAD_ARCHIVE.key(), true)
                .append(HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_SIZE.key(), samplesPerSplit)
                .append(HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_EXTRA_SPLITS.key(), extraSplits)
                .append(HadoopVariantStorageOptions.ARCHIVE_TABLE_PRESPLIT_SIZE.key(), archiveSplitsPerBatch)
                .append(HadoopVariantStorageOptions.ARCHIVE_TABLE_PRESPLIT_EXTRA_SPLITS.key(), extraSplits)
                .append(HadoopVariantStorageOptions.ARCHIVE_FILE_BATCH_SIZE.key(), 2)
                .append(HadoopVariantStorageOptions.EXPECTED_SAMPLES_NUMBER.key(), 1)
                .append(HadoopVariantStorageOptions.EXPECTED_FILES_NUMBER.key(), 1);


        // -- Batch 1
        int batches = 1;
        runETL(engine, getPlatinumFile(1), outputUri, params);
        // Each batch starts with one extra split, expect the first batch. So, -1
        // Then, a fixed number of extra splits
        checkArchiveTableSplits(((archiveSplitsPerBatch + 1) * batches) - 1 + extraSplits);
        checkSampleIndexTableSplits(batches + extraSplits);

        // -- Batch 2
        // First batch has 1 fewer elements than the rest of the batches.
        batches = 2;
        runETL(engine, getPlatinumFile(2), outputUri, params);
        checkArchiveTableSplits(((archiveSplitsPerBatch + 1) * batches) - 1 + extraSplits);
        checkSampleIndexTableSplits(batches + extraSplits);

        runETL(engine, getPlatinumFile(3), outputUri, params);
        checkArchiveTableSplits(((archiveSplitsPerBatch + 1) * batches) - 1 + extraSplits);
        checkSampleIndexTableSplits(batches + extraSplits);

        // -- Batch 3
        batches = 3;
        runETL(engine, getPlatinumFile(4), outputUri, params);
        checkArchiveTableSplits(((archiveSplitsPerBatch + 1) * batches) - 1 + extraSplits);
        checkSampleIndexTableSplits(batches + extraSplits);

        runETL(engine, getPlatinumFile(5), outputUri, params);
        checkArchiveTableSplits(((archiveSplitsPerBatch + 1) * batches) - 1 + extraSplits);
        checkSampleIndexTableSplits(batches + extraSplits);

        // -- Batch 4
        batches = 4;
        runETL(engine, getPlatinumFile(6), outputUri, params);
        checkArchiveTableSplits(((archiveSplitsPerBatch + 1) * batches) - 1 + extraSplits);
        checkSampleIndexTableSplits(batches + extraSplits);

//        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
    }

    private void checkArchiveTableSplits(int expectedSplits) throws Exception {
        int studyId = engine.getMetadataManager().getStudyId(STUDY_NAME);

        String archiveTableName = engine.getArchiveTableName(studyId);
        HBaseManager hBaseManager = dbAdaptor.getHBaseManager();
        int archiveNumRegions = hBaseManager.act(archiveTableName,
                (table, admin) -> HBaseCompat.getInstance().getTableStartKeys(admin, table).length);
        // numRegions == numSplits + 1
        assertEquals(archiveTableName, expectedSplits + 1, archiveNumRegions);
    }

    private void checkSampleIndexTableSplits(int expectedSplits) throws Exception {
        int studyId = engine.getMetadataManager().getStudyId(STUDY_NAME);

        String sampleIndexTableName = sampleIndexDBAdaptor.getSampleIndexTableName(studyId, 1);
        HBaseManager hBaseManager = dbAdaptor.getHBaseManager();
        int sampleIndexNumRegions = hBaseManager.act(sampleIndexTableName,
                (table, admin) -> HBaseCompat.getInstance().getTableStartKeys(admin, table).length);
        // numRegions == numSplits + 1
        assertEquals(sampleIndexTableName, expectedSplits + 1, sampleIndexNumRegions);
    }


}
