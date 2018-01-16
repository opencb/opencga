package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.opencb.commons.io.DataReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created on 16/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseDataReader implements DataReader<Result> {

    private final HBaseManager hBaseManager;
    private final String tableName;
    private final Scan scan;
    private Table table;
    private ResultScanner scanner;

    public HBaseDataReader(HBaseManager hBaseManager, String tableName, Scan scan) {
        this.hBaseManager = hBaseManager;
        this.tableName = tableName;
        this.scan = scan;
    }

    @Override
    public boolean open() {
        try {
            table = hBaseManager.getConnection().getTable(TableName.valueOf(tableName));
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            scanner.close();
            table.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public List<Result> read(int num) {
        try {
            return Arrays.asList(scanner.next(num));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
