package org.opencb.opencga.storage.hadoop.variant.annotation.pending;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class PendingVariantsToAnnotateUtils {
    public static final byte[] FAMILY = GenomeHelper.COLUMN_FAMILY_BYTES;
    public static final byte[] COLUMN = Bytes.toBytes("v");
    public static final byte[] VALUE = new byte[0];

    private static Logger logger = LoggerFactory.getLogger(PendingVariantsToAnnotateUtils.class);

    private PendingVariantsToAnnotateUtils() {
    }

    public static boolean createTableIfNeeded(String pendingAnnotationTableName, HBaseManager hBaseManager) throws IOException {
        HBaseVariantTableNameGenerator.checkValidPendingAnnotationTableName(pendingAnnotationTableName);
        Configuration conf = hBaseManager.getConf();
        List<byte[]> preSplits = new LinkedList<>();
        for (int i = 1; i < 22; i++) {
            preSplits.add(VariantPhoenixKeyFactory.generateVariantRowKey(String.valueOf(i), 0));
        }
        preSplits.add(VariantPhoenixKeyFactory.generateVariantRowKey("X", 0));
        new GenomeHelper(conf);
        return hBaseManager.createTableIfNeeded(pendingAnnotationTableName,
                GenomeHelper.COLUMN_FAMILY_BYTES, preSplits,
                Compression.getCompressionAlgorithmByName(
                        conf.get(
                                HadoopVariantStorageOptions.PENDING_ANNOTATION_TABLE_COMPRESSION.key(),
                                HadoopVariantStorageOptions.PENDING_ANNOTATION_TABLE_COMPRESSION.defaultValue())));
    }

}
