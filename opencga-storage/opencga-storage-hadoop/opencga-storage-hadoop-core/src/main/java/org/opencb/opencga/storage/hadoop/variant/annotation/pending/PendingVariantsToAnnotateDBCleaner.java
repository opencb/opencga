package org.opencb.opencga.storage.hadoop.variant.annotation.pending;

import org.apache.hadoop.hbase.client.Delete;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

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

    public PendingVariantsToAnnotateDBCleaner(HBaseManager hBaseManager, String pendingAnnotationTable) {
        super(hBaseManager, pendingAnnotationTable);
        HBaseVariantTableNameGenerator.checkValidPendingAnnotationTableName(pendingAnnotationTable);
    }

    @Override
    public boolean pre() {
        try {
            PendingVariantsToAnnotateUtils.createTableIfNeeded(tableName, hBaseManager);
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
            Delete delete = new Delete(rowKey);
            deletes.add(delete);
        }
        return deletes;
    }
}
