package org.opencb.opencga.storage.hadoop.variant.adaptors;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Created on 16/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSourceDBAdaptor implements VariantSourceDBAdaptor {

    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantSourceDBAdaptor.class);

    private final GenomeHelper genomeHelper;
    private final ObjectMapper objectMapper;

    public HadoopVariantSourceDBAdaptor(Configuration configuration) {
        this(new GenomeHelper(configuration));
    }

    public HadoopVariantSourceDBAdaptor(Connection connection, Configuration configuration) {
        this(new GenomeHelper(configuration, connection));
    }

    public HadoopVariantSourceDBAdaptor(GenomeHelper genomeHelper) {
        this.genomeHelper = genomeHelper;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
    }

    @Override
    public QueryResult<Long> count() {
        throw new UnsupportedOperationException();
    }

    public VcfMeta getVcfMeta(int studyId, int fileId, QueryOptions options) throws IOException {
        return new VcfMeta(getVariantSource(studyId, fileId, options));
    }

    public VariantSource getVariantSource(int studyId, int fileId, QueryOptions options)
            throws IOException {
        return iterator(studyId, Collections.singletonList(fileId), options).next();
    }

    @Override
    public Iterator<VariantSource> iterator(Query query, QueryOptions options) throws IOException {
        int studyId = query.getInt(VariantSourceQueryParam.STUDY_ID.key());
        List<Integer> fileIds = query.getAsIntegerList(VariantSourceQueryParam.FILE_ID.key());
        return iterator(studyId, fileIds, options);
    }

    public Iterator<VariantSource> iterator(int studyId, QueryOptions options) throws IOException {
        return iterator(studyId, Collections.emptyList(), options);
    }

    public Iterator<VariantSource> iterator(int studyId, List<Integer> fileIds, QueryOptions options) throws IOException {
        String tableName = HadoopVariantStorageManager.getArchiveTableName(studyId, genomeHelper.getConf());
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
            return Collections.emptyIterator();
        }
        HBaseManager.HBaseTableFunction<Result> resultHBaseTableFunction = table -> table.get(get);
        Result result = hBaseManager.act(tableName, resultHBaseTableFunction);
        logger.debug("Get VcfMeta from : {}", fileIds);
        if (result.isEmpty()) {
            return Collections.emptyIterator();
        } else {
            return result.getFamilyMap(genomeHelper.getColumnFamily()).entrySet()
                    .stream()
                    .filter(entry -> !Arrays.equals(entry.getKey(), genomeHelper.getMetaRowKey()))
                    .map(entry -> {
                        try {
                            return objectMapper.readValue(entry.getValue(), VariantSource.class);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .iterator();
        }
//        } catch (IOException e) {
//            throw new StorageManagerException("Error fetching VariantSources from study " + studyId
//                    + ", from table \"" + tableName + "\""
//                    + " for files " + fileIds, e);
//        }
    }

    @Override
    public QueryResult updateSourceStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration,
                                         QueryOptions queryOptions) {
//        String tableName = HadoopVariantStorageManager.getTableName(Integer.parseInt(variantSource.getStudyId()));
        logger.warn("Unimplemented method!");
        return null;
    }

    protected HBaseManager getHBaseManager() {
        return this.genomeHelper.getHBaseManager();
    }


    public void updateVcfMetaData(VcfMeta meta) throws IOException {
        Objects.requireNonNull(meta);
        update(meta.getVariantSource());
    }

    @Override
    public void updateVariantSource(VariantSource variantSource) throws StorageManagerException {
        try {
            update(variantSource);
        } catch (IOException e) {
            throw new StorageManagerException("Unable to update VariantSoruce " + variantSource, e);
        }
    }

    public void update(VariantSource variantSource) throws IOException {
        Objects.requireNonNull(variantSource);
        String tableName = HadoopVariantStorageManager.getArchiveTableName(Integer.parseInt(variantSource.getStudyId()),
                genomeHelper.getConf());
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

    public void updateLoadedFilesSummary(int studyId, List<Integer> newLoadedFiles) throws IOException {
        String tableName = HadoopVariantStorageManager.getArchiveTableName(studyId, genomeHelper.getConf());
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

    public Set<Integer> getLoadedFiles(int studyId) throws IOException {
        String tableName = HadoopVariantStorageManager.getArchiveTableName(studyId, genomeHelper.getConf());
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
