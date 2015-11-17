package org.opencb.opencga.storage.hadoop.variant.archive;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created on 16/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ArchiveFileMetadataManager implements AutoCloseable {


    private final String tableName;
    private final Configuration configuration;
    private final ObjectMap options;
    private final GenomeHelper genomeHelper;
    private final Connection connection;
    private final ObjectMapper objectMapper;
    private final HBaseManager hBaseManager;

    private final Logger logger = LoggerFactory.getLogger(ArchiveDriver.class);

    public ArchiveFileMetadataManager(HadoopCredentials credentials, Configuration configuration, ObjectMap options)
            throws IOException {
        this(credentials.getTable(), configuration, options);
    }

    public ArchiveFileMetadataManager(String tableName, Configuration configuration, ObjectMap options)
            throws IOException {
        this(ConnectionFactory.createConnection(configuration), tableName, configuration, options);
    }

    public ArchiveFileMetadataManager(Connection con, String tableName, Configuration configuration, ObjectMap options)
        throws IOException {
        this.tableName = tableName;
        this.configuration = configuration;
        this.options = options == null? new ObjectMap() : options;
        genomeHelper = new GenomeHelper(configuration);
        connection = con;
        objectMapper = new ObjectMapper();
        hBaseManager = new HBaseManager(configuration);
    }

    public QueryResult<VcfMeta> getAllVcfMetas(ObjectMap options) throws IOException {
        return getVcfMeta(Collections.emptyList(), options);
    }

    public QueryResult<VcfMeta> getVcfMeta(int fileId, ObjectMap options) throws IOException {
        return getVcfMeta(Collections.singletonList(fileId), options);
    }

    public QueryResult<VcfMeta> getVcfMeta(List<Integer> fileIds, ObjectMap options) throws IOException {
        long start = System.currentTimeMillis();
        Get get = new Get(genomeHelper.getMetaRowKey());
        if (fileIds == null || fileIds.isEmpty()) {
            get.addFamily(genomeHelper.getColumnFamily());
        } else {
            for (Integer fileId : fileIds) {
                byte[] columnName = Bytes.toBytes(ArchiveHelper.getColumnName(fileId));
                get.addColumn(genomeHelper.getColumnFamily(), columnName);
            }
        }
        HBaseManager.HBaseTableFunction<Result> resultHBaseTableFunction = table -> table.get(get);
        Result result = hBaseManager.act(connection, tableName, resultHBaseTableFunction);

        if (result.isEmpty()) {
            return new QueryResult<>("getVcfMeta", (int) (System.currentTimeMillis() - start), 0, 0, "", "",
                    Collections.emptyList());
        } else {
            List<VcfMeta> metas = new ArrayList<>(result.size());
            for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(genomeHelper.getColumnFamily()).entrySet()) {
                VariantSource variantSource = objectMapper.readValue(entry.getValue(), VariantSource.class);
                VcfMeta vcfMeta = new VcfMeta(variantSource);
                metas.add(vcfMeta);
            }
            return new QueryResult<>("getVcfMeta", (int) (System.currentTimeMillis() - start), 1, 1, "", "",
                    metas);
        }
    }

    public void updateVcfMetaData(VcfMeta meta) throws IOException {
        Objects.requireNonNull(meta);
        updateVcfMetaData(meta.getVariantSource());
    }

    public void updateVcfMetaData(VariantSource variantSource) throws IOException {
        if( hBaseManager.createTableIfNeeded(connection, tableName, genomeHelper.getColumnFamily())){
            logger.info("Create table '{}' in hbase!", tableName);
        }
        Put put = new Put(genomeHelper.getMetaRowKey());
        put.addColumn(genomeHelper.getColumnFamily(), Bytes.toBytes(variantSource.getFileId()), objectMapper.writeValueAsBytes(variantSource));
        hBaseManager.act(connection, tableName, table -> {
            table.put(put);
        });
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
