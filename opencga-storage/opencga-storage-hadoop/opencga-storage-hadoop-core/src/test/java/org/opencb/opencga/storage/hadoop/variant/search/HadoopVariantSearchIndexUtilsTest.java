package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.SyncStatus;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.INDEX_NOT_SYNC;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.INDEX_STUDIES;

public class HadoopVariantSearchIndexUtilsTest {


    @Test
    public void getSyncStatusCheckStudies() {

        assertEquals(SyncStatus.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(12, Result.create(Arrays.asList(kv(10, INDEX_STUDIES), kv(9, INDEX_NOT_SYNC)))));

        assertEquals(SyncStatus.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(8, Result.create(Arrays.asList(kv(10, INDEX_STUDIES), kv(9, INDEX_NOT_SYNC)))));

        assertEquals(SyncStatus.NOT_SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(8, result(
                        kv(10, INDEX_STUDIES),
                        kv(11, INDEX_NOT_SYNC))));

        assertEquals(SyncStatus.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(0, result(
                        kv(5, INDEX_STUDIES),
                        kv(1, INDEX_NOT_SYNC))));

        assertEquals(SyncStatus.NOT_SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(0, result(
                        kv(1, INDEX_STUDIES),
                        kv(5, INDEX_NOT_SYNC))));

        assertEquals(SyncStatus.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(10, result(
                        kv(5, INDEX_STUDIES),
                        kv(1, INDEX_NOT_SYNC))));

        assertEquals(SyncStatus.SYNCHRONIZED,
                HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(10, result(
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