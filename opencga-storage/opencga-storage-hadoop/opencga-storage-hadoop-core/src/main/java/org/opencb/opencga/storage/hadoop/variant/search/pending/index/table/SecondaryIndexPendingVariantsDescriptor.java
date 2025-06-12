package org.opencb.opencga.storage.hadoop.variant.search.pending.index.table;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.search.VariantSearchSyncInfo;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsTableBasedDescriptor;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;
import java.util.function.Function;

public class SecondaryIndexPendingVariantsDescriptor implements PendingVariantsTableBasedDescriptor {

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
        return HadoopVariantSearchIndexUtils.configureScan(scan, metadataManager);
    }

    public Function<Result, Mutation> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite) {
        if (overwrite) {
            // When overwriting mark all variants as pending
            return (value) -> getMutation(value, true);
        } else {
            long ts = metadataManager.getProjectMetadata().getSecondaryAnnotationIndex()
                    .getLastStagingOrActiveIndex().getLastUpdateDateTimestamp();
            return (value) -> {
                VariantSearchSyncInfo.Status syncStatus = HadoopVariantSearchIndexUtils.getSyncStatusInfoResolved(ts, value);
                boolean pending = syncStatus != VariantSearchSyncInfo.Status.SYNCHRONIZED;
                return getMutation(value, pending);
            };
        }
    }

    private Mutation getMutation(Result value, boolean pending) {
        if (pending) {
            Put put = new Put(value.getRow());
            for (Cell cell : value.rawCells()) {
                put.addImmutable(GenomeHelper.COLUMN_FAMILY_BYTES,
                        CellUtil.getQualifierBufferShallowCopy(cell),
                        // Do not copy the timestamp!
                        HConstants.LATEST_TIMESTAMP,
                        CellUtil.getValueBufferShallowCopy(cell));
            }
            return put;
        } else {
//            return new Delete(value.getRow());
            return null;
        }
    }
}
