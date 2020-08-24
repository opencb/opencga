package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface PendingVariantsDescriptor {

    String name();

    void checkValidPendingTableName(String tableName);

    String getTableName(HBaseVariantTableNameGenerator generator);

    boolean createTableIfNeeded(String tableName, HBaseManager hBaseManager) throws IOException;

    Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager);

    Function<Result, Mutation> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite);

    default boolean createTableIfNeeded(String tableName, HBaseManager hBaseManager, Compression.Algorithm compression) throws IOException {
        checkValidPendingTableName(tableName);
        Configuration conf = hBaseManager.getConf();
        List<byte[]> preSplits = new LinkedList<>();
        for (int i = 1; i < 22; i++) {
            preSplits.add(VariantPhoenixKeyFactory.generateVariantRowKey(String.valueOf(i), 0));
        }
        preSplits.add(VariantPhoenixKeyFactory.generateVariantRowKey("X", 0));
        new GenomeHelper(conf);
        return hBaseManager.createTableIfNeeded(tableName,
                GenomeHelper.COLUMN_FAMILY_BYTES, preSplits, compression);
    }
}
