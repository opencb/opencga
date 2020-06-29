package org.opencb.opencga.storage.hadoop.variant.annotation.pending;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDescriptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.VariantColumn.SO;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.VariantColumn.TYPE;

public class AnnotationPendingVariantsDescriptor implements PendingVariantsDescriptor {

    private static final byte[] SO_BYTES = SO.bytes();

    private static Logger logger = LoggerFactory.getLogger(AnnotationPendingVariantsDescriptor.class);

    @Override
    public String name() {
        return "annotation";
    }

    public void checkValidPendingTableName(String tableName) {
        HBaseVariantTableNameGenerator.checkValidPendingAnnotationTableName(tableName);
    }

    public String getTableName(HBaseVariantTableNameGenerator generator) {
        return generator.getPendingAnnotationTableName();
    }



    public boolean createTableIfNeeded(String tableName, HBaseManager hBaseManager) throws IOException {
        return createTableIfNeeded(tableName, hBaseManager, Compression.getCompressionAlgorithmByName(
                hBaseManager.getConf().get(
                        HadoopVariantStorageOptions.PENDING_ANNOTATION_TABLE_COMPRESSION.key(),
                        HadoopVariantStorageOptions.PENDING_ANNOTATION_TABLE_COMPRESSION.defaultValue())));
    }

    public Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager) {
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, TYPE.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, SO.bytes());
        return scan;
    }

    public boolean isPending(Result value) {
        for (Cell cell : value.rawCells()) {
            if (cell.getValueLength() > 0) {
                if (Bytes.equals(
                        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        SO_BYTES, 0, SO_BYTES.length)) {
                    return false;
                }
            }
        }
        return true;
    }

}
