package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.ProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.hadoop.utils.HBaseLock;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseProjectMetadataDBAdaptor extends ProjectMetadataAdaptor {

    private static Logger logger = LoggerFactory.getLogger(HBaseStudyConfigurationDBAdaptor.class);

    private final Configuration configuration;
    private final HBaseManager hBaseManager;
    private final ObjectMapper objectMapper;
    private final String tableName;
    private final HBaseLock lock;
    private Boolean tableExists = null; // unknown
    private byte[] family;


    public HBaseProjectMetadataDBAdaptor(VariantTableHelper helper) {
        this(helper.getMetaTableAsString(), helper.getConf(), null);
    }

    public HBaseProjectMetadataDBAdaptor(String metaTableName, Configuration configuration, HBaseManager hBaseManager) {
        this.configuration = Objects.requireNonNull(configuration);
        this.tableName = Objects.requireNonNull(metaTableName);
        HBaseVariantTableNameGenerator.checkValidMetaTableName(metaTableName);
        family = new GenomeHelper(configuration).getColumnFamily();
        this.objectMapper = new ObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(configuration);
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
        lock = new HBaseLock(this.hBaseManager, this.tableName, family, null);
    }

    @Override
    protected long lockProject(long lockDuration, long timeout) throws InterruptedException, TimeoutException, StorageEngineException {
        try {
            return lock.lock(getProjectRowKey(), getLockColumn(), lockDuration, timeout);
        } catch (IOException e) {
            throw new StorageEngineException("Error locking project in HBase", e);
        }
    }

    @Override
    protected void unLockProject(long lockId) throws StorageEngineException {
        try {
            lock.unlock(getProjectRowKey(), getLockColumn(), lockId);
        } catch (IOException e) {
            throw new StorageEngineException("Error locking project in HBase", e);
        }
    }

    @Override
    protected QueryResult<ProjectMetadata> getProjectMetadata() {
        try {
            ensureTableExists();
            ProjectMetadata projectMetadata = hBaseManager.act(tableName, (table -> {
                Result result = table.get(new Get(getProjectRowKey()));
                if (result != null) {
                    byte[] value = result.getValue(family, getValueColumn());
                    if (value != null && value.length > 0) {
                        ProjectMetadata projectMetadata1 = objectMapper.readValue(value, ProjectMetadata.class);
                        System.out.println("PROJECT METADATA -> " + Bytes.toString(value));
                        return projectMetadata1;
                    }
                }
                System.out.println("NO PROJECT METADATA");
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
    protected QueryResult updateProjectMetadata(ProjectMetadata projectMetadata) {
        try {
            ensureTableExists();
            hBaseManager.act(tableName, (table -> {
                System.out.println("UPDATE PROJECT METADATA");
                Put put = new Put(getProjectRowKey());
                put.addColumn(family, getValueColumn(), objectMapper.writeValueAsBytes(projectMetadata));
                put.addColumn(family, getTypeColumn(), Type.PROJECT.bytes());
                put.addColumn(family, getStatusColumn(), Status.READY.bytes());
                table.put(put);
            }));

            return new QueryResult<>("");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void ensureTableExists() throws IOException {
        if (tableExists == null || !tableExists) {
            if (createMetaTableIfNeeded(hBaseManager, tableName, family)) {
                logger.info("Create table '{}' in hbase!", tableName);
            }
            tableExists = true;
        }
    }
}
