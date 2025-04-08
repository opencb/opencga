package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseDataReader extends AbstractHBaseDataReader<Result> {

    public HBaseDataReader(HBaseManager hBaseManager, String tableName, Scan scan) {
        super(hBaseManager, tableName, scan);
    }

    @Override
    protected List<Result> convert(List<Result> results) {
        return results;
    }
}
