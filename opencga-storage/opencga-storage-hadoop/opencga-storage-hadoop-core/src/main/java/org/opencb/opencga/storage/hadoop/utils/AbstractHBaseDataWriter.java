package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorImpl;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.io.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Logger logger = LoggerFactory.getLogger(AbstractHBaseDataWriter.class);

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

    protected void mutate(List<M> mutations) throws IOException {
        try {
            mutator.mutate(mutations);
        } catch (IllegalArgumentException e) {
            // Try to extend the information regarding the InvalidArgumentException, in case of being a PUT validation exception
            if (CollectionUtils.isNotEmpty(mutations) && mutations.get(0) instanceof Put && mutator instanceof BufferedMutatorImpl) {
                for (M mutation : mutations) {
                    if (mutation instanceof Put) {
                        try {
                            ((BufferedMutatorImpl) mutator).validatePut(((Put) mutation));
                        } catch (RuntimeException e1) {
                            // Don't print the whole stacktrace
                            logger.error("Invalid put operation on RowKey '" + Bytes.toStringBinary(mutation.getRow()) + "', "
                                    + e1.getMessage());
                        }
                    }
                }
            }
            throw e;
        }
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
