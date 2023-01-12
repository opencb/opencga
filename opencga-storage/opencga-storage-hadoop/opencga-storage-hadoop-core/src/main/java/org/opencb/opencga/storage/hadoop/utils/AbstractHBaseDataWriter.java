package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
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

    protected abstract List<M> convert(List<T> batch) throws IOException;

    @Override
    public boolean open() {
        try {
            mutator = hBaseManager.getConnection().getBufferedMutator(buildBufferedMutatorParams());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to connect to Hbase", e);
        }
        return true;
    }

    protected BufferedMutatorParams buildBufferedMutatorParams() {
        return new BufferedMutatorParams(TableName.valueOf(tableName));
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

    protected final synchronized void mutate(List<M> mutations) throws IOException {
        try {
            mutator.mutate(mutations);
        } catch (IllegalArgumentException e) {
            // Try to extend the information regarding the InvalidArgumentException, in case of being a PUT validation exception
            if (CollectionUtils.isNotEmpty(mutations) && mutations.get(0) instanceof Put && mutator instanceof BufferedMutatorImpl) {
                try {
                    hBaseManager.act(tableName, table -> {
                        for (M mutation : mutations) {
                            if (mutation instanceof Put) {
                                try {
                                    int maxKeyValueSize = hBaseManager.getConf().getInt(
                                            ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY,
                                            ConnectionConfiguration.MAX_KEYVALUE_SIZE_DEFAULT);
                                    validatePut((Put) mutation, maxKeyValueSize);
                                } catch (RuntimeException e1) {
                                    // Don't print the whole stacktrace
                                    logger.error("Invalid put operation on RowKey '" + Bytes.toStringBinary(mutation.getRow()) + "', "
                                            + e1.getMessage());
                                }
                            }
                        }
                    });
                } catch (IOException ioe) {
                    // Do not propagate this IOException.
                    logger.error("Unexpected IOException", e);
                }
            }
            throw e;
        }
    }

    // validate for well-formedness
    public static void validatePut(Put put, int maxKeyValueSize) throws IllegalArgumentException {
        if (put.isEmpty()) {
            throw new IllegalArgumentException("No columns to insert");
        }
        if (maxKeyValueSize > 0) {
            for (List<Cell> list : put.getFamilyCellMap().values()) {
                for (Cell cell : list) {
                    long length = 8L + 12
                            + cell.getRowLength()
                            + cell.getFamilyLength()
                            + cell.getQualifierLength()
                            + cell.getValueLength();
                    if (length > maxKeyValueSize) {
                        throw new IllegalArgumentException("KeyValue size too large");
                    }
                }
            }
        }
    }

    public final synchronized void flush() {
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

    public static List<Put> splitDensePuts(Put put, Configuration conf) throws IOException {
        return splitDensePuts(put, conf.getInt("hbase.region.store.parallel.put.limit.min.column.count", 100));
    }

    public static List<Put> splitDensePuts(Put put, int limitColumnsPerPut) throws IOException {
        List<Put> puts = new ArrayList<>();
        List<Cell> cells = put.getFamilyCellMap().get(GenomeHelper.COLUMN_FAMILY_BYTES);
        List<List<Cell>> lists = BatchUtils.splitBatches(cells, limitColumnsPerPut);
        if (lists.size() > 1) {
            for (List<Cell> list : lists) {
                Put partialPut = new Put(put.getRow());
                for (Cell cell : list) {
                    partialPut.add(cell);
                }
                puts.add(partialPut);
            }
        } else {
            puts.add(put);
        }
        return puts;
    }
}
