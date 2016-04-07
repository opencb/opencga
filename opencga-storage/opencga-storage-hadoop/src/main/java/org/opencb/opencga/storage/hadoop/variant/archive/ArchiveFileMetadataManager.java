package org.opencb.opencga.storage.hadoop.variant.archive;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created on 16/11/15.
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

    public ArchiveFileMetadataManager(HBaseCredentials credentials, Configuration configuration, ObjectMap options)
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
        this.options = options == null ? new ObjectMap() : options;
        genomeHelper = new GenomeHelper(configuration);
        connection = con;
        objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
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
        if (!hBaseManager.act(connection, tableName, (table, admin) -> admin.tableExists(table.getName()))) {
            return new QueryResult<>("getVcfMeta", (int) (System.currentTimeMillis() - start), 0, 0, "", "",
                    Collections.emptyList());
        }
        HBaseManager.HBaseTableFunction<Result> resultHBaseTableFunction = table -> table.get(get);
        Result result = hBaseManager.act(connection, tableName, resultHBaseTableFunction);
        logger.debug("Get VcfMeta from : {}", fileIds);
        if (result.isEmpty()) {
            return new QueryResult<>("getVcfMeta", (int) (System.currentTimeMillis() - start), 0, 0, "", "",
                    Collections.emptyList());
        } else {
            List<VcfMeta> metas = new ArrayList<>(result.size());
            for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(genomeHelper.getColumnFamily()).entrySet()) {
                if (Arrays.equals(entry.getKey(), genomeHelper.getMetaRowKey())) {
                    continue;
                }
                VariantSource variantSource = objectMapper.readValue(entry.getValue(), VariantSource.class);
                logger.debug("Got VcfMeta from : {}, [{}]", variantSource.getFileName(), variantSource.getFileId());
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
        if (ArchiveDriver.createArchiveTableIfNeeded(genomeHelper, tableName, connection)) {
            logger.info("Create table '{}' in hbase!", tableName);
        }
        Put put = new Put(genomeHelper.getMetaRowKey());
        put.addColumn(genomeHelper.getColumnFamily(), Bytes.toBytes(variantSource.getFileId()),
                variantSource.getImpl().toString().getBytes());
        hBaseManager.act(connection, tableName, table -> {
            table.put(put);
        });
    }

    public void updateLoadedFilesSummary(List<Integer> newLoadedFiles) throws IOException {
        if (ArchiveDriver.createArchiveTableIfNeeded(genomeHelper, tableName, connection)) {
            logger.info("Create table '{}' in hbase!", tableName);
        }
        StringBuilder sb = new StringBuilder();
        for (Integer newLoadedFile : newLoadedFiles) {
            sb.append(",").append(newLoadedFile);
        }

        Append append = new Append(genomeHelper.getMetaRowKey());
        append.add(genomeHelper.getColumnFamily(), genomeHelper.getMetaRowKey(),
                Bytes.toBytes(sb.toString()));
        hBaseManager.act(connection, tableName, table -> {
            table.append(append);
        });
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    public Set<Integer> getLoadedFiles() throws IOException {
        if (!hBaseManager.tableExists(connection, tableName)) {
            return new HashSet<>();
        } else {
            return hBaseManager.act(connection, tableName, table -> {
                Get get = new Get(genomeHelper.getMetaRowKey());
                get.addColumn(genomeHelper.getColumnFamily(), genomeHelper.getMetaRowKey());
                byte[] value = table.get(get).getValue(genomeHelper.getColumnFamily(), genomeHelper.getMetaRowKey());
                Set<Integer> set;
                if (value != null) {
                    set = new LinkedHashSet<Integer>();
                    for (String s : Bytes.toString(value).split(",")) {
                        if (!s.isEmpty()) {
                            if (s.startsWith("[")) {
                                s = s.replaceFirst("[", "");
                            }
                            if (s.endsWith("]")) {
                                s = s.replaceAll("]", "");
                            }
                            set.add(Integer.parseInt(s));
                        }
                    }
                } else {
                    set = new LinkedHashSet<Integer>();
                }
                return set;
            });
        }
    }
}
