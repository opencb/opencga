package org.opencb.opencga.storage.hadoop.variant.annotation.pending;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class PendingVariantsToAnnotateUtils {
    public static final byte[] FAMILY = Bytes.toBytes("a");
    public static final byte[] COLUMN = Bytes.toBytes("v");
    public static final byte[] VALUE = new byte[0];

    private static Logger logger = LoggerFactory.getLogger(PendingVariantsToAnnotateUtils.class);

    private PendingVariantsToAnnotateUtils() {
    }

    public static boolean createColumnFamilyIfNeeded(String variantTable, HBaseManager hBaseManager) throws IOException {
        return hBaseManager.act(variantTable, (table, admin) -> {
            for (HColumnDescriptor columnFamily : table.getTableDescriptor().getColumnFamilies()) {
                if (Bytes.equals(columnFamily.getName(), PendingVariantsToAnnotateUtils.FAMILY)) {
                    // Column family already exists
//                    logger.info("Column family '" + Bytes.toString(PendingVariantsToAnnotateUtils.FAMILY) + "' already exists.");
                    return false;
                }
            }
            logger.info("Create column family '" + Bytes.toString(PendingVariantsToAnnotateUtils.FAMILY) + "'.");
            admin.addColumn(table.getName(), new HColumnDescriptor(PendingVariantsToAnnotateUtils.FAMILY));
            return true;
        });
    }

}
