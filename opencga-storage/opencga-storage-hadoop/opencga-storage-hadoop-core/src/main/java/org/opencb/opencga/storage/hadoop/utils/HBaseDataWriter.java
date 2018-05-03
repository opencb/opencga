package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.client.Mutation;

import java.util.List;

/**
 * Created on 01/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseDataWriter<T extends Mutation> extends AbstractHBaseDataWriter<T, T> {

    public HBaseDataWriter(HBaseManager hBaseManager, String tableName) {
        super(hBaseManager, tableName);
    }

    @Override
    protected List<T> convert(List<T> batch) {
        return batch;
    }

}
