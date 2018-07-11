package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;
import org.opencb.commons.io.DataWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

/**
 * Created on 31/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractHBaseDataWriter<T, M extends Mutation> implements DataWriter<T> {

    protected final HBaseManager hBaseManager;
    protected final String tableName;
    private BufferedMutator mutator;

    public AbstractHBaseDataWriter(HBaseManager hBaseManager, String tableName) {
        this.hBaseManager = new HBaseManager(hBaseManager);
        this.tableName = tableName;
    }

    protected abstract List<M> convert(List<T> batch);

    @Override
    public boolean open() {
        try {
            mutator = hBaseManager.getConnection().getBufferedMutator(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to connect to Hbase", e);
        }
        return true;
    }

    @Override
    public boolean write(List<T> list) {
        try {
            mutate(convert(list));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean write(T elem) {
        try {
            mutate(convert(Collections.singletonList(elem)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    protected void mutate(List<M> convert) throws IOException {
        mutator.mutate(convert);
    }

    @Override
    public boolean post() {
        try {
            mutator.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            mutator.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }
}
