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

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created on 16/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantFileMetadataDBAdaptor extends AbstractHBaseDBAdaptor implements VariantFileMetadataDBAdaptor {

    protected static Logger logger = LoggerFactory.getLogger(HBaseVariantFileMetadataDBAdaptor.class);

    public HBaseVariantFileMetadataDBAdaptor(VariantTableHelper helper) {
        super(helper);
    }

    @Deprecated
    public HBaseVariantFileMetadataDBAdaptor(Configuration configuration) {
        // FIXME
        this(null, new HBaseVariantTableNameGenerator(HBaseVariantTableNameGenerator
                .getDBNameFromVariantsTableName(new VariantTableHelper(configuration).getVariantsTableAsString()), configuration)
                .getMetaTableName(), configuration);
    }

    public HBaseVariantFileMetadataDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        super(hBaseManager, metaTableName, configuration);
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
            get.addColumn(family, getValueColumn());
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
                    filterList.addFilter(new RowFilter(CompareOp.EQUAL,
                            new BinaryComparator(getVariantFileMetadataRowKey(studyId, fileId))));
                }
                scan.setFilter(filterList);
            } else {
                scan.setFilter(
                        new SingleColumnValueFilter(
                                family, getTypeColumn(),
                                CompareOp.EQUAL, Type.VARIANT_FILE_METADATA.bytes()));

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
        byte[] value = result.getValue(family, getValueColumn());
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

    private void update(String studyId, VariantFileMetadata metadata) throws IOException {
        Objects.requireNonNull(metadata);
        ensureTableExists();
        Integer fileId = Integer.valueOf(metadata.getId());
        checkFileId(fileId);
        Put put = new Put(getVariantFileMetadataRowKey(Integer.valueOf(studyId), fileId));
        put.addColumn(this.family, getValueColumn(), metadata.getImpl().toString().getBytes());
        put.addColumn(this.family, getTypeColumn(), Type.VARIANT_FILE_METADATA.bytes());
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
        ensureTableExists();

        List<Put> puts = new ArrayList<>(newLoadedFiles.size());
        for (Integer newLoadedFile : newLoadedFiles) {
            Put put = new Put(getVariantFileMetadataRowKey(studyId, newLoadedFile));
            put.addColumn(family, getTypeColumn(), Type.VARIANT_FILE_METADATA.bytes());
            put.addColumn(family, getStatusColumn(), Status.READY.bytes());
            puts.add(put);
        }
        hBaseManager.act(tableName, table -> {
            table.put(puts);
        });
    }

    @Override
    public void delete(int study, int file) throws IOException {
        Delete delete = new Delete(getVariantFileMetadataRowKey(study, file));

        hBaseManager.act(tableName, table -> {
            table.delete(delete);
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
                Set<Integer> set = new TreeSet<>();
                Scan scan = new Scan();
                scan.addColumn(family, getStatusColumn());
                scan.addColumn(family, getTypeColumn());
                scan.setRowPrefixFilter(getVariantFileMetadataRowKeyPrefix(studyId));
                scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
                        new SingleColumnValueFilter(family, getStatusColumn(), CompareOp.EQUAL, Status.READY.bytes()),
                        new SingleColumnValueFilter(family, getTypeColumn(), CompareOp.EQUAL, Type.VARIANT_FILE_METADATA.bytes())));
                for (Result result : table.getScanner(scan)) {
                    Pair<Integer, Integer> pair = parseVariantFileMetadataRowKey(result.getRow());
                    Integer fileId = pair.getValue();
                    set.add(fileId);
                }
                return set;
            });
        }
    }

}
