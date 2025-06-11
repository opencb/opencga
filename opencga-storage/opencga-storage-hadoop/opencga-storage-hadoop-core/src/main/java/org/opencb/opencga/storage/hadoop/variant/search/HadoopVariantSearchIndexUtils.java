package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchSyncStatus;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUpdateDocument;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.*;

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

    public static Put updateSyncStatus(VariantSearchUpdateDocument updateDocument, Map<String, Integer> studiesMap) {
        List<String> studies = updateDocument.getStudies();
        Set<Integer> studyIds = studies.stream().map(o -> studiesMap.get(o.toString())).collect(Collectors.toSet());
        byte[] bytes = PhoenixHelper.toBytes(studyIds, PIntegerArray.INSTANCE);

        byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(updateDocument.getVariant());
        Put put = new Put(row)
                .addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.INDEX_STUDIES.bytes(), bytes);
        return put;
    }

    public static VariantSearchSyncStatus getSyncStatusCheckStudies(long ts, Result result) {
        VariantSearchSyncStatus syncStatus = getSyncStatus(ts, result);
        if (syncStatus == VariantSearchSyncStatus.STUDIES_UNKNOWN_SYNC
                || syncStatus == VariantSearchSyncStatus.STATS_NOT_SYNC_AND_STUDIES_UNKNOWN) {
            Cell studiesCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STUDIES.bytes());
            List<Integer> indexedStudies = AbstractPhoenixConverter.toList((PhoenixArray) VariantColumn.INDEX_STUDIES.getPDataType()
                    .toObject(studiesCell.getValueArray(), studiesCell.getValueOffset(), studiesCell.getValueLength()));
            Set<Integer> actualStudies = new VariantRow(result).getStudies();
            if (indexedStudies.size() == actualStudies.size() && actualStudies.containsAll(indexedStudies)) {
                if (syncStatus == VariantSearchSyncStatus.STUDIES_UNKNOWN_SYNC) {
                    return VariantSearchSyncStatus.SYNCHRONIZED;
                } else {
                    return VariantSearchSyncStatus.STATS_NOT_SYNC;
                }
            } else {
                return VariantSearchSyncStatus.NOT_SYNCHRONIZED;
            }
        }
        return syncStatus;
    }

    public static VariantSearchSyncStatus getSyncStatus(long ts, Result result) {
        Cell studiesCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STUDIES.bytes());
        Cell notSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_NOT_SYNC.bytes());
        Cell statsNotSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STATS_NOT_SYNC.bytes());
        Cell studiesUnknownCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_UNKNOWN.bytes());
        return getSyncStatus(ts, studiesCell, notSyncCell, statsNotSyncCell, studiesUnknownCell);
    }

    public static class VariantSearchSyncInfo {
        private final VariantSearchSyncStatus syncStatus;
        private final List<Integer> studies;

        public VariantSearchSyncInfo(VariantSearchSyncStatus syncStatus, List<Integer> studies) {
            this.syncStatus = syncStatus;
            this.studies = studies;
        }

        public VariantSearchSyncStatus getSyncStatus() {
            return syncStatus;
        }

        public List<Integer> getStudies() {
            return studies;
        }
    }

    public static VariantSearchSyncInfo getSyncInformation(long ts, ResultSet resultSet) throws SQLException {
        Array studiesValue = resultSet.getArray(VariantColumn.INDEX_STUDIES.column());
        List<Integer> studies;
        if (studiesValue != null) {
            studies = AbstractPhoenixConverter.toList((PhoenixArray) studiesValue);
        } else {
            studies = null;
        }

        boolean noSync = resultSet.getBoolean(VariantColumn.INDEX_NOT_SYNC.column());
        boolean statsNotSync = resultSet.getBoolean(VariantColumn.INDEX_STATS_NOT_SYNC.column());
        boolean unknown = resultSet.getBoolean(VariantColumn.INDEX_UNKNOWN.column());

        VariantSearchSyncStatus status = HadoopVariantSearchIndexUtils.getSyncStatus(noSync, unknown, statsNotSync, studies);
        return new VariantSearchSyncInfo(status, studies);

    }

    public static VariantSearchSyncInfo getSyncInformation(long ts, Result result) {
        Cell studiesCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STUDIES.bytes());
        Cell notSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_NOT_SYNC.bytes());
        Cell statsNotSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STATS_NOT_SYNC.bytes());
        Cell studiesUnknownCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_UNKNOWN.bytes());

        List<Integer> studies;
        if (studiesCell != null && studiesCell.getValueLength() != 0) {
            studies = AbstractPhoenixConverter.toList((PhoenixArray) VariantColumn.INDEX_STUDIES.getPDataType()
                    .toObject(studiesCell.getValueArray(), studiesCell.getValueOffset(), studiesCell.getValueLength()));
        } else {
            studies = null;
        }

        VariantSearchSyncStatus status = getSyncStatus(ts, studiesCell, notSyncCell, statsNotSyncCell, studiesUnknownCell);
        return new VariantSearchSyncInfo(status, studies);
    }

    public static VariantSearchSyncStatus getSyncStatus(long ts, Cell studiesCell, Cell notSyncCell,
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

    public static VariantSearchSyncStatus getSyncStatus(boolean noSync, boolean studiesUnknown, boolean statsNotSync,
                                                        List<Integer> studies) {
        return getSyncStatus(noSync, studiesUnknown, statsNotSync, studies != null && !studies.isEmpty());
    }

    public static VariantSearchSyncStatus getSyncStatus(boolean noSync, boolean studiesUnknown, boolean statsNotSync,
                                                        boolean hasStudies) {
        final VariantSearchSyncStatus syncStatus;
        if (noSync) {
            // Some process marked this variant as not synchronized. No doubts here.
            syncStatus = VariantSearchSyncStatus.NOT_SYNCHRONIZED;
        } else if (!hasStudies) {
            // If the list of studies is not present, this variant had never been loaded into solr.
            syncStatus = VariantSearchSyncStatus.NOT_SYNCHRONIZED;
        } else if (statsNotSync) {
            if (studiesUnknown) {
                // Stats unsync, but we do not know the studies.
                syncStatus = VariantSearchSyncStatus.STATS_NOT_SYNC_AND_STUDIES_UNKNOWN;
            } else {
                // Only the stats are not synchronized.
                syncStatus = VariantSearchSyncStatus.STATS_NOT_SYNC;
            }
        } else if (studiesUnknown) {
            // Unknown level of synchronization.
            syncStatus = VariantSearchSyncStatus.STUDIES_UNKNOWN_SYNC;
        } else {
            // If noSync, unknown or statsNotSync are false, then the variant is synchronized
            syncStatus = VariantSearchSyncStatus.SYNCHRONIZED;
        }
        return syncStatus;
    }

    public static Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager) {
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, TYPE.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, ALLELES.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, FULL_ANNOTATION.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, ANNOTATION_ID.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_NOT_SYNC.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_STATS_NOT_SYNC.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_UNKNOWN.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_STUDIES.bytes());
        for (Integer studyId : metadataManager.getStudyIds()) {
            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStudyColumn(studyId).bytes());
            for (CohortMetadata cohort : metadataManager.getCalculatedOrPartialCohorts(studyId)) {
                scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStatsColumn(studyId, cohort.getId()).bytes());
            }
        }
        return scan;
    }
}
