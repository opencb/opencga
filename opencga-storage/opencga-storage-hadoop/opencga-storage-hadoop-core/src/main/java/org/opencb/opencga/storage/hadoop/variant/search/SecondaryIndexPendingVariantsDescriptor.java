package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDescriptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_INDEX_LAST_TIMESTAMP;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.STUDY_SUFIX_BYTES;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.VariantColumn.*;

public class SecondaryIndexPendingVariantsDescriptor implements PendingVariantsDescriptor {

    @Override
    public String name() {
        return "secondary_index";
    }

    @Override
    public void checkValidPendingTableName(String tableName) {
        HBaseVariantTableNameGenerator.checkValidPendingSecondaryIndexTableName(tableName);
    }

    @Override
    public String getTableName(HBaseVariantTableNameGenerator generator) {
        return generator.getPendingSecondaryIndexTableName();
    }

    @Override
    public boolean createTableIfNeeded(String tableName, HBaseManager hBaseManager) throws IOException {
        return createTableIfNeeded(tableName, hBaseManager, Compression.getCompressionAlgorithmByName(
                hBaseManager.getConf().get(
                        HadoopVariantStorageOptions.PENDING_SECONDARY_INDEX_TABLE_COMPRESSION.key(),
                        HadoopVariantStorageOptions.PENDING_SECONDARY_INDEX_TABLE_COMPRESSION.defaultValue())));
    }

    @Override
    public Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager) {
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, TYPE.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, FULL_ANNOTATION.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, ANNOTATION_ID.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_NOT_SYNC.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_UNKNOWN.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_STUDIES.bytes());
        for (Integer studyId : metadataManager.getStudyIds()) {
            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixHelper.getStudyColumn(studyId).bytes());
            for (CohortMetadata cohort : metadataManager.getCalculatedCohorts(studyId)) {
                scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixHelper.getStatsColumn(studyId, cohort.getId()).bytes());
            }
        }
        return scan;
    }

    public Function<Result, Mutation> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite) {
        if (overwrite) {
            // When overwriting mark all variants as pending
            return (value) -> getMutation(value, true);
        } else {
            long ts = metadataManager.getProjectMetadata().getAttributes().getLong(SEARCH_INDEX_LAST_TIMESTAMP.key());
            return (value) -> {
                VariantStorageEngine.SyncStatus syncStatus = HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(ts, value);
                boolean pending = syncStatus != VariantStorageEngine.SyncStatus.SYNCHRONIZED;
                return getMutation(value, pending);
            };
        }
    }

    @Deprecated
    private boolean isPending(Result value, long ts) {
        final boolean pending;
        boolean unknown = false;
        int studies = 0;
        int indexedStudies = 0;
        long nosyncTs = 0;
        long unknownTs = 0;
        long indexedStudiesTs = 0;

        for (Cell cell : value.rawCells()) {
            if (CellUtil.matchingQualifier(cell, INDEX_NOT_SYNC.bytes())) {
//                if (cell.getTimestamp() > ts) {
//                    return true;
//                }
                nosyncTs = cell.getTimestamp();
            } else if (CellUtil.matchingQualifier(cell, INDEX_UNKNOWN.bytes())) {
//                if (cell.getTimestamp() > ts) {
//                }
                unknownTs = cell.getTimestamp();
            } else if (CellUtil.matchingQualifier(cell, INDEX_STUDIES.bytes())) {
                indexedStudiesTs = cell.getTimestamp();
                indexedStudies++;
                for (int i = cell.getValueOffset(); i < cell.getValueLength(); i++) {
                    if (cell.getValueArray()[i] == ',') {
                        indexedStudies++;
                    }
                }
            } else if (AbstractPhoenixConverter.endsWith(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                    STUDY_SUFIX_BYTES)) {
                studies++;
            }
        }
        if (nosyncTs > indexedStudiesTs) {
            // Valid noSync column
            pending = true;
        } else {
            if (unknownTs > indexedStudies) {
                // Valid unknown column
                unknown = true;
            }
            if (unknown) {
                // If the field indexedStudies exists, and it contains same number of studies studies should have, skip this variant
                // Pending if number of studies is different.
                pending = indexedStudies != studies;
            } else {
                pending = false;
            }
        }
        return pending;
    }

    private Mutation getMutation(Result value, boolean pending) {
        if (pending) {
            Put put = new Put(value.getRow());
            try {
                for (Cell cell : value.rawCells()) {
                    put.add(cell);
                }
            } catch (IOException e) {
                // This should never happen
                throw new UncheckedIOException(e);
            }
            return put;
        } else {
//            return new Delete(value.getRow());
            return null;
        }
    }
}
