package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.search.VariantSearchSyncInfo;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.INDEX_NOT_SYNC;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.INDEX_STUDIES;

@Category(ShortTests.class)
public class HadoopVariantSearchIndexUtilsTest {


    @Test
    public void testNoAvailableInformationDefaultNotSync() {
        // No available information, default NOT_SYNC
        assertEquals(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 12, result()));
    }

    @Test
    public void testOldNotSyncFlagMissingStudies() {
        // Old NOT_SYNC flag. Missing STUDIES.
        // No available information, default NOT_SYNC
        assertEquals(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 12, result(kv(9, INDEX_NOT_SYNC))));
    }

    @Test
    public void testOldNotSyncFlagValidStudiesList() {
        // Old NOT_SYNC flag.
        // Valid STUDIES list.
        assertEquals(VariantSearchSyncInfo.Status.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 12, result(kv(10, INDEX_STUDIES), kv(9, INDEX_NOT_SYNC))));
    }

    @Test
    public void testValidNotSyncFlagNewerStudiesSynchronized() {
        // Valid NOT_SYNC flag.
        // Valid STUDIES list -> newer -> SYNCHRONIZED
        assertEquals(VariantSearchSyncInfo.Status.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 8, result(
                        kv(10, INDEX_STUDIES),
                        kv(9, INDEX_NOT_SYNC))));
    }

    @Test
    public void testValidStudiesNewerCreationDateSynchronized() {
        assertEquals(VariantSearchSyncInfo.Status.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 0, result(
                        kv(10, INDEX_STUDIES))));

        assertEquals(VariantSearchSyncInfo.Status.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 0, result(
                        kv(10, INDEX_STUDIES), kv(8, INDEX_NOT_SYNC))));

        assertEquals(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(20, 0, result(
                        kv(10, INDEX_STUDIES))));
    }

    @Test
    public void testValidNotSyncFlagNewerNotSynchronized() {
        // Valid NOT_SYNC flag -> newer -> NOT_SYNCHRONIZED
        // Valid STUDIES list.
        assertEquals(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 8, result(
                        kv(9, INDEX_STUDIES),
                        kv(10, INDEX_NOT_SYNC))));
    }

    @Test
    public void getSyncStatusCheckStudies() {

        assertEquals(VariantSearchSyncInfo.Status.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(1, 0, result(
                        kv(50, INDEX_STUDIES),
                        kv(10, INDEX_NOT_SYNC))));

        assertEquals(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(1, 0, result(
                        kv(10, INDEX_STUDIES),
                        kv(50, INDEX_NOT_SYNC))));

        assertEquals(VariantSearchSyncInfo.Status.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 10, result(
                        kv(5, INDEX_STUDIES),
                        kv(1, INDEX_NOT_SYNC))));

        assertEquals(VariantSearchSyncInfo.Status.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(0, 10, result(
                        kv(1, INDEX_STUDIES),
                        kv(5, INDEX_NOT_SYNC))));
    }

    private Result result(Cell... cells) {
        List<Cell> cellsArray = Arrays.asList(cells);
        cellsArray.sort(KeyValue.COMPARATOR);
        return Result.create(cellsArray);
    }

    private Cell kv(long ts, VariantPhoenixSchema.VariantColumn column) {
        return kv(ts, column, "value");
    }

    private Cell kv(long ts, VariantPhoenixSchema.VariantColumn column, String value) {
        return new KeyValue(Bytes.toBytes("row"), GenomeHelper.COLUMN_FAMILY_BYTES, column.bytes(), ts, Bytes.toBytes(value));
    }


}