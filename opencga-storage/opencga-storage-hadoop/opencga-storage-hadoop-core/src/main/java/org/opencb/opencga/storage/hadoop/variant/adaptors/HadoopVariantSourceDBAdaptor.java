/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 16/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSourceDBAdaptor implements VariantSourceDBAdaptor {

    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantSourceDBAdaptor.class);

    private final GenomeHelper genomeHelper;
    private final HBaseManager hBaseManager;
    private final ObjectMapper objectMapper;

    public HadoopVariantSourceDBAdaptor(Configuration configuration) {
        this(new GenomeHelper(configuration), null);
    }

    public HadoopVariantSourceDBAdaptor(GenomeHelper genomeHelper, HBaseManager hBaseManager) {
        this.genomeHelper = genomeHelper;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(genomeHelper.getConf());
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
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
        String tableName = HadoopVariantStorageEngine.getArchiveTableName(studyId, genomeHelper.getConf());
        long start = System.currentTimeMillis();
        Get get = new Get(genomeHelper.getMetaRowKey());
        if (fileIds == null || fileIds.isEmpty()) {
            get.addFamily(genomeHelper.getColumnFamily());
        } else {
            for (Integer fileId : fileIds) {
                byte[] columnName = Bytes.toBytes(ArchiveTableHelper.getColumnName(fileId));
                get.addColumn(genomeHelper.getColumnFamily(), columnName);
            }
        }
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
                            throw new UncheckedIOException("Problem with " + Bytes.toString(entry.getKey()), e);
                        }
                    })
                    .iterator();
        }
//        } catch (IOException e) {
//            throw new StorageEngineException("Error fetching VariantSources from study " + studyId
//                    + ", from table \"" + tableName + "\""
//                    + " for files " + fileIds, e);
//        }
    }

    @Override
    public QueryResult updateSourceStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration,
                                         QueryOptions queryOptions) {
//        String tableName = HadoopVariantStorageEngine.getTableName(Integer.parseInt(variantSource.getStudyId()));
        logger.warn("Unimplemented method!");
        return null;
    }

    public void updateVcfMetaData(VcfMeta meta) throws IOException {
        Objects.requireNonNull(meta);
        update(meta.getVariantSource());
    }

    @Override
    public void updateVariantSource(VariantSource variantSource) throws StorageEngineException {
        try {
            update(variantSource);
        } catch (IOException e) {
            throw new StorageEngineException("Unable to update VariantSoruce " + variantSource, e);
        }
    }

    public void update(VariantSource variantSource) throws IOException {
        Objects.requireNonNull(variantSource);
        String tableName = HadoopVariantStorageEngine.getArchiveTableName(Integer.parseInt(variantSource.getStudyId()),
                genomeHelper.getConf());
        if (ArchiveTableHelper.createArchiveTableIfNeeded(genomeHelper, tableName, hBaseManager.getConnection())) {
            logger.info("Create table '{}' in hbase!", tableName);
        }
        Put put = wrapVcfMetaAsPut(variantSource, this.genomeHelper);
        hBaseManager.act(tableName, table -> {
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
        String tableName = HadoopVariantStorageEngine.getArchiveTableName(studyId, genomeHelper.getConf());
        if (ArchiveTableHelper.createArchiveTableIfNeeded(genomeHelper, tableName, hBaseManager.getConnection())) {
            logger.info("Create table '{}' in hbase!", tableName);
        }
        StringBuilder sb = new StringBuilder();
        for (Integer newLoadedFile : newLoadedFiles) {
            sb.append(",").append(newLoadedFile);
        }

        Append append = new Append(genomeHelper.getMetaRowKey());
        append.add(genomeHelper.getColumnFamily(), genomeHelper.getMetaRowKey(),
                Bytes.toBytes(sb.toString()));
        hBaseManager.act(tableName, table -> {
            table.append(append);
        });
    }

    @Override
    public void delete(int study, int file) throws IOException {
        String tableName = HadoopVariantStorageEngine.getArchiveTableName(study, genomeHelper.getConf());

        Set<Integer> loadedFiles = getLoadedFiles(study);
        loadedFiles.remove(file);
        String loadedFilesStr = loadedFiles.stream().map(Object::toString).collect(Collectors.joining(","));

        // Remove from loaded files
        Put putLoadedFiles = new Put(genomeHelper.getMetaRowKey());
        putLoadedFiles.addColumn(genomeHelper.getColumnFamily(), genomeHelper.getMetaRowKey(),
                Bytes.toBytes(loadedFilesStr));

        Delete delete = new Delete(genomeHelper.getMetaRowKey())
                .addColumn(genomeHelper.getColumnFamily(), Bytes.toBytes(String.valueOf(file)));
        hBaseManager.act(tableName, table -> {
            table.delete(delete);
            table.put(putLoadedFiles);
        });
    }

    @Override
    public void close() throws IOException {
        try {
            hBaseManager.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public Set<Integer> getLoadedFiles(int studyId) throws IOException {
        String tableName = HadoopVariantStorageEngine.getArchiveTableName(studyId, genomeHelper.getConf());
        if (!hBaseManager.tableExists(tableName)) {
            return new HashSet<>();
        } else {
            return hBaseManager.act(tableName, table -> {
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
