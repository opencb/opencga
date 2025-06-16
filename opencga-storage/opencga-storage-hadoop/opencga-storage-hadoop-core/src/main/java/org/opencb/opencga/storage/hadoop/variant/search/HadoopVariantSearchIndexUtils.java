package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchSyncInfo;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUpdateDocument;
import org.opencb.opencga.storage.core.variant.search.VariantSecondaryIndexFilter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
     */
    public static void addNotSyncStatus(Put put) {
        if (put != null) {
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_NOT_SYNC.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
    }

    /**
     * Marks the row as Stats Not Sync Status. This method should be called when loading statistics.
     *
     * @param put Mutation to add new Variant information.
     */
    public static void addStatsNotSyncStatus(Put put) {
        if (put != null) {
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STATS_NOT_SYNC.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
    }

    /**
     * Marks the row as Unknown Sync Status. This method should be called when loading or removing files.
     *
     * @param put Mutation to add new Variant information.
     */
    public static void addUnknownSyncStatus(Put put) {
        if (put != null) {
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_UNKNOWN.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
    }

    public static Put updateSyncStatus(VariantSearchUpdateDocument updateDocument) {
        byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(updateDocument.getVariant());
        Put put = new Put(row);

        Set<Integer> studyIds = updateDocument.getSyncInfo().getStudies();

        Map<Integer, Long> statsHash = updateDocument.getSyncInfo().getStatsHash();
        StringBuilder sb = new StringBuilder();
        if (statsHash != null && !statsHash.isEmpty()) {
            for (Map.Entry<Integer, Long> entry : statsHash.entrySet()) {
                Integer cohortId = entry.getKey();
                Long hash = entry.getValue();
                sb.append(cohortId).append(":").append(hash).append(",");
            }
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.INDEX_STATS.bytes(),
                    Bytes.toBytes(sb.substring(0, sb.length() - 1)));
        }

        byte[] bytes = PhoenixHelper.toBytes(studyIds, PIntegerArray.INSTANCE);
        put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.INDEX_STUDIES.bytes(), bytes);

        return put;
    }

    /**
     * Get the synchronization status of a variant, given the timestamp and the result.
     * Resolve the uncertain status. No "UNKNOWN" status will be returned.
     *
     * @param ts        LastUpdateDateTimestamp
     * @param result    HBase result.
     * @return          Resolved status
     */
    public static VariantSearchSyncInfo.Status getSyncStatusInfoResolved(long ts, Result result) {
        return getSyncStatusInfoResolved(ts, result, null);
    }

    /**
     * Get the synchronization status of a variant, given the timestamp and the result.
     * Resolve the uncertain status. No "UNKNOWN" status will be returned.
     *
     * @param ts        LastUpdateDateTimestamp
     * @param result    HBase result.
     * @param cohortSizeMap Map with cohort id and cohort size.
     * @return          Resolved status
     */
    public static VariantSearchSyncInfo.Status getSyncStatusInfoResolved(long ts, Result result, Map<Integer, Integer> cohortSizeMap) {
        VariantSearchSyncInfo info = getSyncInformation(ts, result);
        VariantSearchSyncInfo.Status status = info.getStatus();
        if (status.isUnknown()) {
            VariantRow variantRow = new VariantRow(result);
            if (status.studiesUnknown()) {
                Set<Integer> indexedStudies = info.getStudies();
                Set<Integer> actualStudies = variantRow.getStudies();

                if (!actualStudies.equals(indexedStudies)) {
                    return VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED;
                } else if (status == VariantSearchSyncInfo.Status.STUDIES_UNKNOWN) {
                    return VariantSearchSyncInfo.Status.SYNCHRONIZED;
                } else if (status == VariantSearchSyncInfo.Status.STATS_AND_STUDIES_UNKNOWN) {
                    status = VariantSearchSyncInfo.Status.STATS_UNKNOWN;
                }
            }

            Map<Integer, Long> cohortHash = new HashMap<>();
            if (cohortSizeMap != null) {
                variantRow.walker().onCohortStats(statsColumn -> {
                    int cohortId = statsColumn.getCohortId();
                    VariantStats variantStats = statsColumn.toJava();
                    int statsHashKey = VariantSecondaryIndexFilter.getStatsHashKey(statsColumn.getStudyId(), statsColumn.getCohortId());
                    long statsHash = VariantSecondaryIndexFilter.getStatsHashValue(variantStats, cohortSizeMap.get(cohortId));
                    cohortHash.put(statsHashKey, statsHash);
                }).walk();
            }
            if (cohortHash.equals(info.getStatsHash())) {
                return VariantSearchSyncInfo.Status.SYNCHRONIZED;
            } else {
                return VariantSearchSyncInfo.Status.STATS_NOT_SYNC;
            }
        }
        return status;
    }

//    public static VariantSearchSyncInfo getSyncInformation(long ts, ResultSet resultSet) throws SQLException {
//        Array studiesValue = resultSet.getArray(VariantColumn.INDEX_STUDIES.column());
//        Set<Integer> studies;
//        if (studiesValue != null) {
//            studies = new HashSet<>(AbstractPhoenixConverter.toList((PhoenixArray) studiesValue));
//        } else {
//            studies = null;
//        }
//
//        boolean noSync = resultSet.getBoolean(VariantColumn.INDEX_NOT_SYNC.column());
//        boolean statsNotSync = resultSet.getBoolean(VariantColumn.INDEX_STATS_NOT_SYNC.column());
//        boolean unknown = resultSet.getBoolean(VariantColumn.INDEX_UNKNOWN.column());
//
//        VariantSearchSyncInfo.Status status = HadoopVariantSearchIndexUtils.getSyncStatus(noSync, unknown, statsNotSync, studies);
//        return new VariantSearchSyncInfo(status, studies, null);
//    }

    public static VariantSearchSyncInfo getSyncInformation(long ts, Result result) {
        Cell studiesCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STUDIES.bytes());
        Cell notSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_NOT_SYNC.bytes());
        Cell statsNotSyncCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STATS_NOT_SYNC.bytes());
        Cell statsCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_STATS.bytes());
        Cell studiesUnknownCell = result.getColumnLatestCell(GenomeHelper.COLUMN_FAMILY_BYTES, VariantColumn.INDEX_UNKNOWN.bytes());

        Set<Integer> studies;
        if (studiesCell != null && studiesCell.getValueLength() != 0) {
            studies = new HashSet<>(AbstractPhoenixConverter.toList((PhoenixArray) VariantColumn.INDEX_STUDIES.getPDataType()
                    .toObject(studiesCell.getValueArray(), studiesCell.getValueOffset(), studiesCell.getValueLength())));
        } else {
            studies = null;
        }
        Map<Integer, Long> statsHash;
        if (statsCell != null && statsCell.getValueLength() != 0) {
            String statsString = Bytes.toString(statsCell.getValueArray(), statsCell.getValueOffset(), statsCell.getValueLength());
            statsHash = new HashMap<>();
            for (String kv : statsString.split(",")) {
                int idx = kv.lastIndexOf(':');
                statsHash.put(Integer.parseInt(kv.substring(0, idx)), Long.valueOf(kv.substring(idx + 1)));
            }
        } else {
            statsHash = null;
        }

        VariantSearchSyncInfo.Status status = getSyncStatus(ts, studiesCell, notSyncCell, statsNotSyncCell, studiesUnknownCell);
        return new VariantSearchSyncInfo(status, studies, statsHash);
    }

    private static VariantSearchSyncInfo.Status getSyncStatus(long ts, Cell studiesCell, Cell notSyncCell,
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

    private static VariantSearchSyncInfo.Status getSyncStatus(boolean noSync, boolean studiesUnknown, boolean statsNotSync,
                                                              Set<Integer> studies) {
        return getSyncStatus(noSync, studiesUnknown, statsNotSync, studies != null && !studies.isEmpty());
    }

    private static VariantSearchSyncInfo.Status getSyncStatus(boolean noSync, boolean studiesUnknown, boolean statsNotSync,
                                                              boolean hasStudies) {
        final VariantSearchSyncInfo.Status syncStatus;
        if (noSync) {
            // Some process marked this variant as not synchronized. No doubts here.
            syncStatus = VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED;
        } else if (!hasStudies) {
            // If the list of studies is not present, this variant had never been loaded into solr.
            syncStatus = VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED;
        } else if (statsNotSync) {
            if (studiesUnknown) {
                // Stats unsync, but we do not know the studies.
                syncStatus = VariantSearchSyncInfo.Status.STATS_AND_STUDIES_UNKNOWN;
            } else {
                // Only the stats are not synchronized.
                syncStatus = VariantSearchSyncInfo.Status.STATS_UNKNOWN;
            }
        } else if (studiesUnknown) {
            // Unknown level of synchronization.
            syncStatus = VariantSearchSyncInfo.Status.STUDIES_UNKNOWN;
        } else {
            // If noSync, unknown or statsNotSync are false, then the variant is synchronized
            syncStatus = VariantSearchSyncInfo.Status.SYNCHRONIZED;
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
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_STATS.bytes());
        for (Integer studyId : metadataManager.getStudyIds()) {
            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStudyColumn(studyId).bytes());
            for (CohortMetadata cohort : metadataManager.getCalculatedOrPartialCohorts(studyId)) {
                scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStatsColumn(studyId, cohort.getId()).bytes());
            }
        }
        return scan;
    }
}
