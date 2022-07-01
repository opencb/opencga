package org.opencb.opencga.storage.hadoop.variant.prune;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDescriptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;
import java.util.function.Function;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SecondaryIndexPrunePendingVariantsDescriptor implements PendingVariantsDescriptor {

    @Override
    public String name() {
        return "prune";
    }

    @Override
    public void checkValidPendingTableName(String tableName) {
        HBaseVariantTableNameGenerator.checkValidPendingSecondaryIndexPruneTableName(tableName);
    }

    @Override
    public String getTableName(HBaseVariantTableNameGenerator generator) {
        return generator.getPendingSecondaryIndexPruneTableName();
    }

    @Override
    public boolean createTableIfNeeded(String tableName, HBaseManager hBaseManager) throws IOException {
        return createTableIfNeeded(tableName, hBaseManager, Compression.getCompressionAlgorithmByName(
                hBaseManager.getConf().get(
                        HadoopVariantStorageOptions.PENDING_SECONDARY_INDEX_PRUNE_TABLE_COMPRESSION.key(),
                        HadoopVariantStorageOptions.PENDING_SECONDARY_INDEX_PRUNE_TABLE_COMPRESSION.defaultValue())));
    }

    @Override
    public Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Function<Result, Mutation> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite) {
        throw new UnsupportedOperationException();
    }
}
