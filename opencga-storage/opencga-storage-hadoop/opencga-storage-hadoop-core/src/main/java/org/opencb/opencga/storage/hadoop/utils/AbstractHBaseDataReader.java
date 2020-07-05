package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.opencb.commons.io.DataReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Created on 16/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractHBaseDataReader<T> implements DataReader<T> {

    protected final HBaseManager hBaseManager;
    protected final String tableName;
    protected final List<Scan> scans;
    protected final Iterator<Scan> scanIterator;
    protected Table table;
    protected long limit = -1;
    protected long numResults = 0;
    protected ResultScanner scanner;

    public AbstractHBaseDataReader(HBaseManager hBaseManager, String tableName, Scan scan) {
        this(hBaseManager, tableName, Collections.singletonList(scan));
    }

    public AbstractHBaseDataReader(HBaseManager hBaseManager, String tableName, List<Scan> scans) {
        this.hBaseManager = hBaseManager;
        this.tableName = tableName;
        this.scans = scans;
        if (scans == null || scans.isEmpty()) {
            throw new IllegalArgumentException("Invalid empty scans list");
        }
        scanIterator = scans.iterator();
    }

    @Override
    public boolean open() {
        try {
            table = hBaseManager.getConnection().getTable(TableName.valueOf(tableName));
            nextScanner();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    private void nextScanner() throws IOException {
        if (scanner != null) {
            scanner.close();
        }
        if (!scanIterator.hasNext()) {
            scanner = null;
        } else {
            scanner = hBaseManager.getScanner(tableName, scanIterator.next());
        }
    }

    @Override
    public boolean close() {
        try {
            if (scanner != null) {
                scanner.close();
            }
            table.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public List<T> read(int num) {
        try {
            List<Result> resultSets = new ArrayList<>(num);
            if (scanner != null) {
                for (int i = 0; i < num; i++) {
                    if (numResults == limit) {
                        break;
                    }
                    Result next = scanner.next();
                    if (next == null) {
                        nextScanner();
                        if (scanner == null) {
                            break;
                        }
                    } else {
                        numResults++;
                        resultSets.add(next);
                    }
                }
            }
            return convert(resultSets);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long getLimit() {
        return limit;
    }

    public AbstractHBaseDataReader<T> setLimit(long limit) {
        this.limit = limit == 0 ? -1 : limit;
        return this;
    }

    protected abstract List<T> convert(List<Result> results);
}
