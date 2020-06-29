package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface PendingVariantsDescriptor {
    byte[] FAMILY = GenomeHelper.COLUMN_FAMILY_BYTES;
    byte[] COLUMN = Bytes.toBytes("v");
    byte[] VALUE = new byte[0];

    void checkValidPendingTableName(String tableName);

    String getTableName(HBaseVariantTableNameGenerator generator);

    boolean createTableIfNeeded(String tableName, HBaseManager hBaseManager) throws IOException;

    Scan configureScan(Scan scan);

    boolean isPending(Result value);
}
