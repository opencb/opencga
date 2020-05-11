package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.utils.iterators.IteratorWithClosable;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.hadoop.utils.HBaseLockManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created on 03/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractHBaseDBAdaptor {

    protected static Logger logger = LoggerFactory.getLogger(AbstractHBaseDBAdaptor.class);

    protected final HBaseManager hBaseManager;
    protected final ObjectMapper objectMapper;
    private final HBaseLockManager lock;
    protected final String tableName;
    private Boolean tableExists = null; // unknown
    protected byte[] family;

    public AbstractHBaseDBAdaptor(VariantTableHelper helper) {
        this(null, helper.getMetaTableAsString(), helper.getConf());
    }

    public AbstractHBaseDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        Objects.requireNonNull(configuration);
        this.tableName = Objects.requireNonNull(metaTableName);
        HBaseVariantTableNameGenerator.checkValidMetaTableName(metaTableName);
        new GenomeHelper(configuration);
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        this.objectMapper = new ObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(configuration);
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
        lock = new HBaseLockManager(this.hBaseManager, this.tableName, family, null);
    }

    protected void ensureTableExists() {
        if (tableExists == null || !tableExists) {
            try {
                if (createMetaTableIfNeeded(hBaseManager, tableName, family)) {
                    logger.info("Create table '{}' in hbase!", tableName);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            tableExists = true;
        }
    }

    protected boolean tableExists() {
        if (tableExists == null || !tableExists) {
            try {
                tableExists = hBaseManager.tableExists(tableName);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return tableExists;
    }

    protected <T> Iterator<T> iterator(byte[] rowKeyPrefix, Class<T> clazz) {
        return iterator(rowKeyPrefix, clazz, getValueColumn(), false);
    }

    protected <T> Iterator<T> iterator(byte[] rowKeyPrefix, Class<T> clazz, boolean reversed) {
        return iterator(rowKeyPrefix, clazz, getValueColumn(), reversed);
    }

    protected <T> Iterator<T> iterator(byte[] rowKeyPrefix, Class<T> clazz, byte[] valueColumn, boolean reversed) {
        return iterator(rowKeyPrefix, clazz, valueColumn, reversed, null);
    }

    protected <T> Iterator<T> iterator(byte[] rowKeyPrefix, Class<T> clazz, byte[] valueColumn, boolean reversed, Filter filter) {
        if (!tableExists()) {
            return Collections.emptyIterator();
        }

//        logger.debug("Get {} {} from DB {}", clazz.getSimpleName(), id, tableName);
        Scan scan = new Scan();
        scan.setReversed(reversed);
        scan.setRowPrefixFilter(rowKeyPrefix);
        if (reversed) {
            byte[] startRow = scan.getStartRow();
            scan.setStartRow(scan.getStopRow());
            scan.setStopRow(startRow);
        }
        scan.addColumn(family, valueColumn);
        if (filter != null) {
            scan.setFilter(filter);
        }

        try {
            return hBaseManager.act(tableName, table -> {
//                logger.info("-------------------------------------");
//                logger.info("##### scan = " + scan);
//                logger.info("##### clazz = " + clazz);
//                logger.info("##### size = " + Iterators.size(table.getScanner(scan).iterator()));
                ResultScanner scanner = table.getScanner(scan);
                return new IteratorWithClosable<>(Iterators.transform(scanner.iterator(), result -> {
                    try {
                        return convertResult(result, clazz);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }), scanner);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected <T> Iterator<T> iterator(Iterator<byte[]> rows, Class<T> clazz, byte[] valueColumn) {
        if (!tableExists()) {
            return Collections.emptyIterator();
        }

        try {
            return hBaseManager.act(tableName, table -> {
                Iterator<Get> getsIterator = Iterators.transform(rows, row -> new Get(row).addColumn(family, valueColumn));
                // Group gets in lists of 100 elements
                UnmodifiableIterator<List<Get>> groupedIterator = Iterators.partition(getsIterator, 100);
                // Table.get
                Iterator<Iterator<Result>> resultsIteratorIterator = Iterators.transform(groupedIterator, subList -> {
                    try {
                        return Iterators.forArray(table.get(subList));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                // Concat and filter out null values
                Iterator<Result> resultsIterator = Iterators.filter(Iterators.concat(resultsIteratorIterator), Objects::nonNull);
                // Convert to clazz
                return Iterators.transform(resultsIterator, result -> {
                    try {
                        return convertResult(result, clazz);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected <T> T readValue(byte[] rowKey, Class<T> clazz) {
        return readValue(rowKey, clazz, null);
    }

    protected <T> T readValue(byte[] rowKey, Class<T> clazz, Long timeStamp) {
        return readValue(rowKey, clazz, timeStamp, getValueColumn());
    }

    protected <T> T readValue(byte[] rowKey, Class<T> clazz, Long timeStamp, byte[] valueColumn) {
        if (!tableExists()) {
            return null;
        }

//        logger.debug("Get {} {} from DB {}", clazz.getSimpleName(), Bytes.toString(rowKey), tableName);
        Get get = new Get(rowKey);
        get.addColumn(family, valueColumn);

        if (timeStamp != null) {
            try {
                get.setTimeRange(timeStamp + 1, Long.MAX_VALUE);
            } catch (IOException e) {
                //This should not happen ever.
                throw new UncheckedIOException(e);
            }
        }

        try {
            return hBaseManager.act(tableName, table -> {
                return convertResult(table.get(get), clazz);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <T> T convertResult(Result result, Class<T> clazz) throws IOException {
        if (result == null || result.isEmpty()) {
            return null;
        } else {
            Cell cell = result.rawCells()[0];
            return objectMapper.readValue(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), clazz);
        }
    }

    protected <T> void putValue(byte[] rowKey, HBaseVariantMetadataUtils.Type type, T value) {
        putValue(rowKey, type, value, null);
    }

    protected <T> void putValue(byte[] rowKey, HBaseVariantMetadataUtils.Type type, T value, Long timeStamp) {
        ensureTableExists();

        if (timeStamp == null) {
            timeStamp = System.currentTimeMillis();
        }

        try {
            Put put = new Put(rowKey);
            put.addColumn(family, getTypeColumn(), timeStamp, type.bytes());
            put.addColumn(family, getValueColumn(), timeStamp, objectMapper.writeValueAsBytes(value));

            hBaseManager.act(tableName, table -> {
                table.put(put);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    protected void deleteRow(byte[] rowKey) {
        try {
            hBaseManager.act(tableName, table -> {
                table.delete(new Delete(rowKey));
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected Lock lock(byte[] rowKey, long lockDuration, long timeout) throws StorageEngineException {
        return lock(rowKey, getLockColumn(), lockDuration, timeout);
    }

    protected Lock lock(byte[] rowKey, byte[] lockName, long lockDuration, long timeout) throws StorageEngineException {
        return lockToken(rowKey, lockName, lockDuration, timeout);
    }

    protected Lock lockToken(byte[] rowKey, byte[] lockName, long lockDuration, long timeout) throws StorageEngineException {
        try {
            ensureTableExists();
            return this.lock.lock(rowKey, lockName, lockDuration, timeout);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }  catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageEngineException("Unable to lock " + Bytes.toString(rowKey), e);
        } catch (TimeoutException e) {
            throw new StorageEngineException("Unable to lock " + Bytes.toString(rowKey), e);
        }
    }

    protected void unLock(byte[] rowKey, byte[] lockName, long token) {
        try {
            this.lock.unlock(rowKey, lockName, token);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }



}
