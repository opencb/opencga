package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.ProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseLockManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseProjectMetadataDBAdaptor extends AbstractHBaseDBAdaptor implements ProjectMetadataAdaptor {

    private static Logger logger = LoggerFactory.getLogger(HBaseStudyMetadataDBAdaptor.class);

    private final HBaseLockManager lock;

    public HBaseProjectMetadataDBAdaptor(VariantTableHelper helper) {
        this(null, helper.getMetaTableAsString(), helper.getConf());
    }

    public HBaseProjectMetadataDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        super(hBaseManager, metaTableName, configuration);
        lock = new HBaseLockManager(this.hBaseManager, this.tableName, family, null);
    }

    @Override
    public Lock lockProject(long lockDuration, long timeout, String lockName)
            throws InterruptedException, TimeoutException, StorageEngineException {
        try {
            ensureTableExists();
            return lock.lock(getProjectRowKey(), getLockColumn(lockName), lockDuration, timeout);
        } catch (IOException e) {
            throw new StorageEngineException("Error locking project in HBase", e);
        }
    }

    @Override
    public void unLockProject(long lockId) throws StorageEngineException {
        try {
            lock.unlock(getProjectRowKey(), getLockColumn(), lockId);
        } catch (IOException e) {
            throw new StorageEngineException("Error locking project in HBase", e);
        }
    }

    @Override
    public DataResult<ProjectMetadata> getProjectMetadata() {
        if (!tableExists()) {
            return new DataResult<>();
        }
        try {
            ProjectMetadata projectMetadata = hBaseManager.act(tableName, (table -> {
                Result result = table.get(new Get(getProjectRowKey()));
                if (result != null) {
                    Map<String, Integer> counters = new HashMap<>();
                    for (Cell cell : result.rawCells()) {
                        byte[] column = CellUtil.cloneQualifier(cell);
                        if (Bytes.startsWith(column, COUNTER_PREFIX_BYTES)) {
                            long c = Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                            counters.put(Bytes.toString(column), (int) c);
                        }
                    }
                    byte[] value = result.getValue(family, getValueColumn());
                    if (value != null && value.length > 0) {
                        ProjectMetadata pm = objectMapper.readValue(value, ProjectMetadata.class);
                        if (pm != null) {
                            pm.setCounters(counters);
                        }
                        return pm;
                    }
                }
                logger.info("ProjectMetadata not found in table " + tableName);
                return null;
            }));

            if (projectMetadata == null) {
                return new DataResult<>();
            } else {
                return new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList(projectMetadata), 1);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public DataResult updateProjectMetadata(ProjectMetadata projectMetadata, boolean updateCounters) {
        try {
            ensureTableExists();
            hBaseManager.act(tableName, (table -> {
                Put put = new Put(getProjectRowKey());
                put.addColumn(family, getValueColumn(), objectMapper.writeValueAsBytes(projectMetadata));
                put.addColumn(family, getTypeColumn(), Type.PROJECT.bytes());
                if (updateCounters) {
                    for (Map.Entry<String, Integer> entry : projectMetadata.getCounters().entrySet()) {
                        put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().longValue()));
                    }
                }
                table.put(put);
            }));

            return new DataResult();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int generateId(Integer studyId, String idType) throws StorageEngineException {
        try {
            ensureTableExists();
            return hBaseManager.act(tableName, (table) -> {
                byte[] column = getCounterColumn(studyId, idType);
                return (int) table.incrementColumnValue(getProjectRowKey(), family, column, 1);

            });
        } catch (IOException e) {
            throw new StorageEngineException("Error generating ID", e);
        }
    }

    @Override
    public boolean exists() {
        return tableExists();
    }

}
