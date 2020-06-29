package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDescriptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;

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
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_NOT_SYNC.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_UNKNOWN.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_STUDIES.bytes());
        for (Integer studyId : metadataManager.getStudyIds()) {
            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixHelper.getStudyColumn(studyId).bytes());
        }
        return scan;
    }

    @Override
    public boolean isPending(Result value) {
        boolean unknown = false;
        int studies = 0;
        int indexedStudies = 0;

        for (Cell cell : value.rawCells()) {
            if (CellUtil.matchingQualifier(cell, INDEX_NOT_SYNC.bytes())) {
                return true;
            } else if (CellUtil.matchingQualifier(cell, INDEX_UNKNOWN.bytes())) {
                unknown = true;
            } else if (CellUtil.matchingQualifier(cell, INDEX_STUDIES.bytes())) {
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
        if (unknown && indexedStudies > 0) {
            // If the field indexedStudies exists, and it contains same number of studies studies should have, skip this variant
            if (indexedStudies == studies) {
                // Variant already synchronized. Skip this variant!
                return false;
            }
        }
        return true;
    }
}
