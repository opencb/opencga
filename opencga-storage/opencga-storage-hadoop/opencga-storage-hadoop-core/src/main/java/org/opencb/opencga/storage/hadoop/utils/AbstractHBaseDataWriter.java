package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;
import org.opencb.commons.io.DataWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

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
        this.hBaseManager = new HBaseManager(Objects.requireNonNull(hBaseManager));
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

    protected void mutate(List<M> convert) throws IOException {
        mutator.mutate(convert);
    }

    public void flush() {
        try {
            mutator.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean post() {
        flush();
        return true;
    }

    @Override
    public boolean close() {
        try {
            mutator.close();
            hBaseManager.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }
}
