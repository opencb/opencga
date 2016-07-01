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
    private final GenomeHelper genomeHelper;
    private final ObjectMapper objectMapper;

    private final Logger logger = LoggerFactory.getLogger(ArchiveDriver.class);

    public ArchiveFileMetadataManager(HBaseCredentials credentials, Configuration configuration)
            throws IOException {
        this(credentials.getTable(), configuration);
    }

    public ArchiveFileMetadataManager(String tableName, Configuration configuration)
            throws IOException {
        this(null, tableName, configuration);
    }

    public ArchiveFileMetadataManager(Connection con, String tableName, Configuration configuration) {
        this.tableName = tableName;
        this.genomeHelper = new GenomeHelper(configuration, con);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
    }

    protected HBaseManager getHBaseManager() {
        return this.genomeHelper.getHBaseManager();
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
        HBaseManager hBaseManager = getHBaseManager();
        if (!hBaseManager.act(tableName, (table, admin) -> admin.tableExists(table.getName()))) {
            return new QueryResult<>("getVcfMeta", (int) (System.currentTimeMillis() - start), 0, 0, "", "",
                    Collections.emptyList());
        }
        HBaseManager.HBaseTableFunction<Result> resultHBaseTableFunction = table -> table.get(get);
        Result result = hBaseManager.act(tableName, resultHBaseTableFunction);
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
        if (ArchiveDriver.createArchiveTableIfNeeded(genomeHelper, tableName, getHBaseManager().getConnection())) {
            logger.info("Create table '{}' in hbase!", tableName);
        }
        Put put = wrapVcfMetaAsPut(variantSource, this.genomeHelper);
        getHBaseManager().act(tableName, table -> {
            table.put(put);
        });
    }

    public static Put wrapVcfMetaAsPut(VariantSource variantSource, GenomeHelper helper) {
        Put put = new Put(helper.getMetaRowKey());
        put.addColumn(helper.getColumnFamily(), Bytes.toBytes(variantSource.getFileId()),
                variantSource.getImpl().toString().getBytes());
        return put;
    }

    public void updateLoadedFilesSummary(List<Integer> newLoadedFiles) throws IOException {
        if (ArchiveDriver.createArchiveTableIfNeeded(genomeHelper, tableName, getHBaseManager().getConnection())) {
            logger.info("Create table '{}' in hbase!", tableName);
        }
        StringBuilder sb = new StringBuilder();
        for (Integer newLoadedFile : newLoadedFiles) {
            sb.append(",").append(newLoadedFile);
        }

        Append append = new Append(genomeHelper.getMetaRowKey());
        append.add(genomeHelper.getColumnFamily(), genomeHelper.getMetaRowKey(),
                Bytes.toBytes(sb.toString()));
        getHBaseManager().act(tableName, table -> {
            table.append(append);
        });
    }

    @Override
    public void close() throws IOException {
        try {
            this.genomeHelper.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public Set<Integer> getLoadedFiles() throws IOException {
        if (!getHBaseManager().tableExists(tableName)) {
            return new HashSet<>();
        } else {
            return getHBaseManager().act(tableName, table -> {
                Get get = new Get(genomeHelper.getMetaRowKey());
                get.addColumn(genomeHelper.getColumnFamily(), genomeHelper.getMetaRowKey());
                byte[] value = table.get(get).getValue(genomeHelper.getColumnFamily(), genomeHelper.getMetaRowKey());
                Set<Integer> set;
                if (value != null) {
                    set = new LinkedHashSet<Integer>();
                    for (String s : Bytes.toString(value).split(",")) {
                        if (!s.isEmpty()) {
                            if (s.startsWith("[")) {
                                s = s.replaceFirst("\\[", "");
                            }
                            if (s.endsWith("]")) {
                                s = s.replaceAll("\\]", "");
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
