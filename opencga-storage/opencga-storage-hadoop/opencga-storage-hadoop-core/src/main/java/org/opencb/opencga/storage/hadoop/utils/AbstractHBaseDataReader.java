package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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
public abstract class AbstractHBaseDataReader<T> implements DataReader<T> {

    protected final HBaseManager hBaseManager;
    protected final String tableName;
    protected final Scan scan;
    protected Table table;
    protected ResultScanner scanner;

    public AbstractHBaseDataReader(HBaseManager hBaseManager, String tableName, Scan scan) {
        this.hBaseManager = hBaseManager;
        this.tableName = tableName;
        this.scan = scan;
    }

    @Override
    public boolean open() {
        try {
            table = hBaseManager.getConnection().getTable(TableName.valueOf(tableName));
            scanner = hBaseManager.getScanner(tableName, scan);
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
    public List<T> read(int num) {
        try {
            return convert(Arrays.asList(scanner.next(num)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected abstract List<T> convert(List<Result> results);
}
