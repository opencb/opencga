package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;

import java.util.List;
import java.util.Set;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn;

/**
 * Created on 23/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchIndexUtils {

    /**
     * Marks the row as Not Sync Status. This method should be called when loading annotations or statistics, and when removing a study.
     *
     * @param put Mutation to add new Variant information.
     * @param columnFamily Main column family.
     * @return The same put operation with the {@link VariantColumn#INDEX_NOT_SYNC} column.
     */
    public static Put addNotSyncStatus(Put put, byte[] columnFamily) {
        if (put != null) {
            put.addColumn(columnFamily, VariantColumn.INDEX_NOT_SYNC.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
        return put;
    }

    /**
     * Marks the row as Unknown Sync Status. This method should be called when loading or removing files.
     *
     * @param put Mutation to add new Variant information.
     * @param columnFamily Main column family.
     * @return The same put operation with the {@link VariantColumn#INDEX_UNKNOWN} column.
     */
    public static Put addUnknownSyncStatus(Put put, byte[] columnFamily) {
        if (put != null) {
            put.addColumn(columnFamily, VariantColumn.INDEX_UNKNOWN.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
        return put;
    }

    public static VariantStorageEngine.SyncStatus getSyncStatusCheckStudies(long ts, Result result) {
        VariantStorageEngine.SyncStatus syncStatus = getSyncStatus(ts, result);
        if (syncStatus.equals(VariantStorageEngine.SyncStatus.UNKNOWN)) {
            Cell studiesCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STUDIES.bytes());
            List<Integer> indexedStudies = AbstractPhoenixConverter.toList((PhoenixArray) VariantColumn.INDEX_STUDIES.getPDataType()
                    .toObject(studiesCell.getValueArray(), studiesCell.getValueOffset(), studiesCell.getValueLength()));
            Set<Integer> actualStudies = new VariantRow(result).getStudies();
            if (indexedStudies.size() == actualStudies.size() && actualStudies.containsAll(indexedStudies)) {
                return VariantStorageEngine.SyncStatus.SYNCHRONIZED;
            } else {
                return VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED;
            }
        }
        return syncStatus;
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(long ts, Result result) {
        Cell studiesCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STUDIES.bytes());
        Cell notSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_NOT_SYNC.bytes());
        Cell unknownCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_UNKNOWN.bytes());
        return getSyncStatus(ts, studiesCell, notSyncCell, unknownCell);
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(long ts, Cell studiesCell, Cell notSyncCell, Cell unknownCell) {
        // Don't need to check the value. If present, only check the timestamp.
        boolean notSync = notSyncCell != null && notSyncCell.getTimestamp() > ts;
        boolean unknown = unknownCell != null && unknownCell.getTimestamp() > ts;
        boolean hasStudies = studiesCell != null && studiesCell.getValueLength() != 0;
        if (studiesCell != null) {
            if (notSync) {
                // NotSyncCell must have a newer timestamp than studiesCell to be valid
                notSync = notSyncCell.getTimestamp() > studiesCell.getTimestamp();
            }
            if (unknown) {
                // UnknownCell must have a newer timestamp than studiesCell to be valid
                unknown = unknownCell.getTimestamp() > studiesCell.getTimestamp();
            }
        }

        return HadoopVariantSearchIndexUtils.getSyncStatus(notSync, unknown, hasStudies);
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(boolean noSync, boolean unknown, List<Integer> studies) {
        return getSyncStatus(noSync, unknown, studies != null && !studies.isEmpty());
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(boolean noSync, boolean unknown, boolean hasStudies) {
        final VariantStorageEngine.SyncStatus syncStatus;
        if (noSync) {
            syncStatus = VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED;
        } else if (!hasStudies) {
            syncStatus = VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED;
        } else if (unknown) {
            syncStatus = VariantStorageEngine.SyncStatus.UNKNOWN;
        } else {
            syncStatus = VariantStorageEngine.SyncStatus.SYNCHRONIZED;

        }
        return syncStatus;
    }
}
