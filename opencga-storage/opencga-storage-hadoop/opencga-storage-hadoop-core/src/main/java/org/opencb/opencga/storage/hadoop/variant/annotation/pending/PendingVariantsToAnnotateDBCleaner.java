package org.opencb.opencga.storage.hadoop.variant.annotation.pending;

import org.apache.hadoop.hbase.client.Delete;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PendingVariantsToAnnotateDBCleaner extends AbstractHBaseDataWriter<byte[], Delete> {

    public PendingVariantsToAnnotateDBCleaner(HBaseManager hBaseManager, String tableName) {
        super(hBaseManager, tableName);
    }

    @Override
    public boolean pre() {
        try {
            PendingVariantsToAnnotateUtils.createColumnFamilyIfNeeded(tableName, hBaseManager);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    protected List<Delete> convert(List<byte[]> batch) {
        List<Delete> deletes = new ArrayList<>(batch.size());
        for (byte[] rowKey : batch) {
//            Delete delete = new Delete(rowKey)
//                    .addColumn(PendingVariantsToAnnotateUtils.FAMILY, PendingVariantsToAnnotateUtils.COLUMN);
            Delete delete = new Delete(rowKey)
                    .addFamily(PendingVariantsToAnnotateUtils.FAMILY);
            deletes.add(delete);
        }
        return deletes;
    }
}
