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
     * Marks the row as Not Sync Status. This method should be called when loading annotations and when removing a study.
     *
     * @param put Mutation to add new Variant information.
     * @return The same put operation with the {@link VariantColumn#INDEX_NOT_SYNC} column.
     */
    public static Put addNotSyncStatus(Put put) {
        if (put != null) {
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_NOT_SYNC.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
        return put;
    }

    /**
     * Marks the row as Stats Not Sync Status. This method should be called when loading statistics.
     *
     * @param put Mutation to add new Variant information.
     * @return The same put operation with the {@link VariantColumn#INDEX_STATS_NOT_SYNC} column.
     */
    public static Put addStatsNotSyncStatus(Put put) {
        if (put != null) {
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STATS_NOT_SYNC.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
        return put;
    }

    /**
     * Marks the row as Unknown Sync Status. This method should be called when loading or removing files.
     *
     * @param put Mutation to add new Variant information.
     * @return The same put operation with the {@link VariantColumn#INDEX_UNKNOWN} column.
     */
    public static Put addUnknownSyncStatus(Put put) {
        if (put != null) {
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_UNKNOWN.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
        return put;
    }

    public static VariantStorageEngine.SyncStatus getSyncStatusCheckStudies(long ts, Result result) {
        VariantStorageEngine.SyncStatus syncStatus = getSyncStatus(ts, result);
        if (syncStatus == VariantStorageEngine.SyncStatus.STUDIES_UNKNOWN_SYNC
                || syncStatus == VariantStorageEngine.SyncStatus.STATS_NOT_SYNC_AND_STUDIES_UNKNOWN) {
            Cell studiesCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STUDIES.bytes());
            List<Integer> indexedStudies = AbstractPhoenixConverter.toList((PhoenixArray) VariantColumn.INDEX_STUDIES.getPDataType()
                    .toObject(studiesCell.getValueArray(), studiesCell.getValueOffset(), studiesCell.getValueLength()));
            Set<Integer> actualStudies = new VariantRow(result).getStudies();
            if (indexedStudies.size() == actualStudies.size() && actualStudies.containsAll(indexedStudies)) {
                if (syncStatus == VariantStorageEngine.SyncStatus.STUDIES_UNKNOWN_SYNC) {
                    return VariantStorageEngine.SyncStatus.SYNCHRONIZED;
                } else {
                    return VariantStorageEngine.SyncStatus.STATS_NOT_SYNC;
                }
            } else {
                return VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED;
            }
        }
        return syncStatus;
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(long ts, Result result) {
        Cell studiesCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STUDIES.bytes());
        Cell notSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_NOT_SYNC.bytes());
        Cell statsNotSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STATS_NOT_SYNC.bytes());
        Cell studiesUnknownCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_UNKNOWN.bytes());
        return getSyncStatus(ts, studiesCell, notSyncCell, statsNotSyncCell, studiesUnknownCell);
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(long ts, Cell studiesCell, Cell notSyncCell,
                                                                Cell statsNotSyncCell, Cell studiesUnknownCell) {
        // Don't need to check the value. If present, only check the timestamp.
        boolean notSync = notSyncCell != null && notSyncCell.getTimestamp() > ts;
        boolean statsNotSync = statsNotSyncCell != null && statsNotSyncCell.getTimestamp() > ts;
        boolean studiesUnknown = studiesUnknownCell != null && studiesUnknownCell.getTimestamp() > ts;
        boolean hasStudies = studiesCell != null && studiesCell.getValueLength() != 0;
        if (studiesCell != null) {
            if (notSync) {
                // NotSyncCell must have a newer timestamp than studiesCell to be valid
                notSync = notSyncCell.getTimestamp() > studiesCell.getTimestamp();
            }
            if (studiesUnknown) {
                // UnknownCell must have a newer timestamp than studiesCell to be valid
                studiesUnknown = studiesUnknownCell.getTimestamp() > studiesCell.getTimestamp();
            }
            if (statsNotSync) {
                // StatsNotSyncCell must have a newer timestamp than studiesCell to be valid
                statsNotSync = statsNotSyncCell.getTimestamp() > studiesCell.getTimestamp();
            }
        }

        return HadoopVariantSearchIndexUtils.getSyncStatus(notSync, studiesUnknown, statsNotSync, hasStudies);
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(boolean noSync, boolean studiesUnknown, boolean statsNotSync,
                                                                List<Integer> studies) {
        return getSyncStatus(noSync, studiesUnknown, statsNotSync, studies != null && !studies.isEmpty());
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(boolean noSync, boolean studiesUnknown, boolean statsNotSync,
                                                                boolean hasStudies) {
        final VariantStorageEngine.SyncStatus syncStatus;
        if (noSync) {
            // Some process marked this variant as not synchronized. No doubts here.
            syncStatus = VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED;
        } else if (!hasStudies) {
            // If the list of studies is not present, this variant had never been loaded into solr.
            syncStatus = VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED;
        } else if (statsNotSync) {
            if (studiesUnknown) {
                // Stats unsync, but we do not know the studies.
                syncStatus = VariantStorageEngine.SyncStatus.STATS_NOT_SYNC_AND_STUDIES_UNKNOWN;
            } else {
                // Only the stats are not synchronized.
                syncStatus = VariantStorageEngine.SyncStatus.STATS_NOT_SYNC;
            }
        } else if (studiesUnknown) {
            // Unknown level of synchronization.
            syncStatus = VariantStorageEngine.SyncStatus.STUDIES_UNKNOWN_SYNC;
        } else {
            // If noSync, unknown or statsNotSync are false, then the variant is synchronized
            syncStatus = VariantStorageEngine.SyncStatus.SYNCHRONIZED;
        }
        return syncStatus;
    }
}
