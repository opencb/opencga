package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.ProjectMetadataAdaptor;
import org.opencb.opencga.storage.hadoop.utils.HBaseLock;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
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

    private static Logger logger = LoggerFactory.getLogger(HBaseStudyConfigurationDBAdaptor.class);

    private final HBaseLock lock;

    public HBaseProjectMetadataDBAdaptor(VariantTableHelper helper) {
        this(null, helper.getMetaTableAsString(), helper.getConf());
    }

    public HBaseProjectMetadataDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        super(hBaseManager, metaTableName, configuration);
        lock = new HBaseLock(this.hBaseManager, this.tableName, family, null);
    }

    @Override
    public long lockProject(long lockDuration, long timeout) throws InterruptedException, TimeoutException, StorageEngineException {
        try {
            ensureTableExists();
            return lock.lock(getProjectRowKey(), getLockColumn(), lockDuration, timeout);
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
    public QueryResult<ProjectMetadata> getProjectMetadata() {
        try {
            ensureTableExists();
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
                return new QueryResult<>("");
            } else {
                return new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(projectMetadata));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public QueryResult updateProjectMetadata(ProjectMetadata projectMetadata, boolean updateCounters) {
        try {
            ensureTableExists();
            hBaseManager.act(tableName, (table -> {
                Put put = new Put(getProjectRowKey());
                put.addColumn(family, getValueColumn(), objectMapper.writeValueAsBytes(projectMetadata));
                put.addColumn(family, getTypeColumn(), Type.PROJECT.bytes());
                put.addColumn(family, getStatusColumn(), Status.READY.bytes());
                if (updateCounters) {
                    for (Map.Entry<String, Integer> entry : projectMetadata.getCounters().entrySet()) {
                        put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().longValue()));
                    }
                }
                table.put(put);
            }));

            return getProjectMetadata();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int generateId(StudyConfiguration studyConfiguration, String idType) throws StorageEngineException {
        try {
            return hBaseManager.act(tableName, (table) -> {
                byte[] column = getCounterColumn(studyConfiguration, idType);
                return (int) table.incrementColumnValue(getProjectRowKey(), family, column, 1);

            });
        } catch (IOException e) {
            throw new StorageEngineException("Error generating ID", e);
        }
    }

}
