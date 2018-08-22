package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.util.List;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationHadoopDBWriter extends HBaseDataWriter<Put> {

    private byte[] columnFamily;

    public VariantAnnotationHadoopDBWriter(HBaseManager hBaseManager, String tableName, byte[] columnFamily) {
        super(hBaseManager, tableName);
        this.columnFamily = columnFamily;
    }

    @Override
    protected List<Put> convert(List<Put> puts) {
        for (Put put : puts) {
            HadoopVariantSearchIndexUtils.addNotSyncStatus(put, columnFamily);
        }

        return puts;
    }
}
