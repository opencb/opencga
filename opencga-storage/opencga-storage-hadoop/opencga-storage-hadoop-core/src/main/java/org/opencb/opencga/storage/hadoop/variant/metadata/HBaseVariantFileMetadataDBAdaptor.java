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

package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created on 16/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantFileMetadataDBAdaptor implements VariantFileMetadataDBAdaptor {

    protected static Logger logger = LoggerFactory.getLogger(HBaseVariantFileMetadataDBAdaptor.class);

    private final GenomeHelper genomeHelper;
    private final HBaseManager hBaseManager;
    private final ObjectMapper objectMapper;
    private final String tableName;

    public HBaseVariantFileMetadataDBAdaptor(Configuration configuration) {
        // FIXME
        this(new GenomeHelper(configuration), null, new HBaseVariantTableNameGenerator(HBaseVariantTableNameGenerator
                .getDBNameFromVariantsTableName(new VariantTableHelper(configuration).getAnalysisTableAsString()), configuration));
    }

    public HBaseVariantFileMetadataDBAdaptor(GenomeHelper genomeHelper, HBaseManager hBaseManager,
                                             HBaseVariantTableNameGenerator tableNameGenerator) {
        this.genomeHelper = genomeHelper;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(genomeHelper.getConf());
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
        tableName = tableNameGenerator.getMetaTableName();
    }

    @Override
    public QueryResult<Long> count(Query query) {
        throw new UnsupportedOperationException();
    }

    public VariantFileMetadata getVariantFileMetadata(int studyId, int fileId, QueryOptions options)
            throws IOException {
        Iterator<VariantFileMetadata> iterator = iterator(studyId, Collections.singletonList(fileId), options);
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            throw VariantQueryException.fileNotFound(fileId, studyId);
        }
    }

    @Override
    public Iterator<VariantFileMetadata> iterator(Query query, QueryOptions options) throws IOException {
        int studyId = query.getInt(VariantFileMetadataQueryParam.STUDY_ID.key());
        List<Integer> fileIds = query.getAsIntegerList(VariantFileMetadataQueryParam.FILE_ID.key());
        return iterator(studyId, fileIds, options);
    }

    public Iterator<VariantFileMetadata> iterator(int studyId, QueryOptions options) throws IOException {
        return iterator(studyId, Collections.emptyList(), options);
    }

    public Iterator<VariantFileMetadata> iterator(int studyId, List<Integer> fileIds, QueryOptions options) throws IOException {
        logger.debug("Get VariantFileMetadata from : {}", fileIds);

        if (!hBaseManager.act(tableName, (table, admin) -> admin.tableExists(table.getName()))) {
            return Collections.emptyIterator();
        }

        if (fileIds != null && fileIds.size() == 1) {
            Get get = new Get(getVariantFileMetadataRowKey(studyId, fileIds.get(0)));
            get.addColumn(genomeHelper.getColumnFamily(), getValueColumn());
            Result result = hBaseManager.act(tableName, (HBaseManager.HBaseTableFunction<Result>) table -> table.get(get));
            VariantFileMetadata value = resultToVariantFileMetadata(result);
            if (value == null) {
                return Collections.emptyIterator();
            } else {
                return Iterators.singletonIterator(value);
            }
        } else {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(getVariantFileMetadataRowKeyPrefix(studyId));
            if (fileIds != null && !fileIds.isEmpty()) {
                FilterList filterList = new FilterList();
                for (Integer fileId : fileIds) {
                    filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
                            new BinaryComparator(getVariantFileMetadataRowKey(studyId, fileId))));
                }
                scan.setFilter(filterList);
            } else {
                scan.setFilter(
                        new SingleColumnValueFilter(
                                genomeHelper.getColumnFamily(), getTypeColumn(),
                                CompareFilter.CompareOp.EQUAL, Type.VARIANT_FILE_METADATA.bytes()));

            }
            ResultScanner scanner = hBaseManager.act(tableName,
                    (HBaseManager.HBaseTableFunction<ResultScanner>) table -> table.getScanner(scan));

            return Iterators.transform(scanner.iterator(), this::resultToVariantFileMetadata);
        }
    }

    private VariantFileMetadata resultToVariantFileMetadata(Result result) {
        if (result == null || result.isEmpty()) {
            return null;
        }
        byte[] value = result.getValue(genomeHelper.getColumnFamily(), getValueColumn());
        try {
            return objectMapper.readValue(value, VariantFileMetadata.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Problem with " + Bytes.toString(result.getRow()), e);
        }
    }

    @Override
    public QueryResult updateStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration,
                                   QueryOptions queryOptions) {
//        String tableName = HadoopVariantStorageEngine.getTableName(Integer.parseInt(variantSource.getStudyId()));
        logger.warn("Unimplemented method!");
        return null;
    }

    @Override
    public void updateVariantFileMetadata(String studyId, VariantFileMetadata metadata) throws StorageEngineException {
        try {
            update(studyId, metadata);
        } catch (IOException e) {
            throw new StorageEngineException("Unable to update VariantFileMetadata " + metadata, e);
        }
    }

    public void update(String studyId, VariantFileMetadata metadata) throws IOException {
        Objects.requireNonNull(metadata);
        if (ArchiveTableHelper.createArchiveTableIfNeeded(genomeHelper, tableName, hBaseManager.getConnection())) {
            logger.info("Create table '{}' in hbase!", tableName);
        }
        Integer fileId = Integer.valueOf(metadata.getId());
        checkFileId(fileId);
        Put put = new Put(getVariantFileMetadataRowKey(Integer.valueOf(studyId), fileId));
        put.addColumn(this.genomeHelper.getColumnFamily(), getValueColumn(), metadata.getImpl().toString().getBytes());
        put.addColumn(this.genomeHelper.getColumnFamily(), getTypeColumn(), Type.VARIANT_FILE_METADATA.bytes());
        hBaseManager.act(tableName, table -> {
            table.put(put);
        });
    }

    public static void checkFileId(int fileId) {
        if (fileId <= 0) {
            throw new IllegalArgumentException("FileId must be greater than 0. Got " + fileId);
        }
    }

    public void updateLoadedFilesSummary(int studyId, List<Integer> newLoadedFiles) throws IOException {
        if (ArchiveTableHelper.createArchiveTableIfNeeded(genomeHelper, tableName, hBaseManager.getConnection())) {
            logger.info("Create table '{}' in hbase!", tableName);
        }
        StringBuilder sb = new StringBuilder();
        for (Integer newLoadedFile : newLoadedFiles) {
            sb.append(',').append(newLoadedFile);
        }

        Append append = new Append(getFilesSummaryRowKey(studyId));
        append.add(genomeHelper.getColumnFamily(), getValueColumn(), Bytes.toBytes(sb.toString()));
        Put put = new Put(getFilesSummaryRowKey(studyId));
        put.addColumn(genomeHelper.getColumnFamily(), getTypeColumn(), Type.FILES.bytes());

        hBaseManager.act(tableName, table -> {
            table.append(append);
            table.put(put);
        });
    }

    @Override
    public void delete(int study, int file) throws IOException {

        Set<Integer> loadedFiles = getLoadedFiles(study);
        loadedFiles.remove(file);
        String loadedFilesStr = loadedFiles.stream().map(Object::toString).collect(Collectors.joining(","));

        // Remove from loaded files
        Put putLoadedFiles = new Put(getFilesSummaryRowKey(study));
        putLoadedFiles.addColumn(genomeHelper.getColumnFamily(), getValueColumn(), Bytes.toBytes(loadedFilesStr));

        Delete delete = new Delete(getVariantFileMetadataRowKey(study, file));

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
        if (!hBaseManager.tableExists(tableName)) {
            return new HashSet<>();
        } else {
            return hBaseManager.act(tableName, table -> {
                Get get = new Get(getFilesSummaryRowKey(studyId));
                get.addColumn(genomeHelper.getColumnFamily(), getValueColumn());

                byte[] value = table.get(get).getValue(genomeHelper.getColumnFamily(), getValueColumn());
                Set<Integer> set;
                if (value != null) {
                    set = new LinkedHashSet<>();
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
                    set = new LinkedHashSet<>();
                }
                return set;
            });
        }
    }
}
