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

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.VariantAnnotationPhoenixDBWriter;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.VariantAnnotationUpsertExecutor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHadoopArchiveDBIterator;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.VariantStatsToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseResultSetIterator;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseScanIterator;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created by mh719 on 16/06/15.
 */
public class VariantHadoopDBAdaptor implements VariantDBAdaptor {
    public static final String NATIVE = "native";
    public static final QueryParam ANNOT_NAME = QueryParam.create("annotName", "", Type.TEXT);

    protected static Logger logger = LoggerFactory.getLogger(VariantHadoopDBAdaptor.class);
    private final String variantTable;
    private final VariantPhoenixHelper phoenixHelper;
    private final HBaseCredentials credentials;
    private final AtomicReference<StudyConfigurationManager> studyConfigurationManager = new AtomicReference<>(null);
    private final Configuration configuration;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final GenomeHelper genomeHelper;
    private final AtomicReference<java.sql.Connection> phoenixCon = new AtomicReference<>();
    private final VariantSqlQueryParser queryParser;
    private final VariantHBaseQueryParser hbaseQueryParser;
    private final HBaseVariantFileMetadataDBAdaptor variantFileMetadataDBAdaptor;
    private final int phoenixFetchSize;
    private boolean clientSideSkip;
    private HBaseManager hBaseManager;

    public VariantHadoopDBAdaptor(HBaseManager hBaseManager, HBaseCredentials credentials, StorageConfiguration configuration,
                                  Configuration conf, CellBaseUtils cellBaseUtils, HBaseVariantTableNameGenerator tableNameGenerator)
            throws IOException {
        this.credentials = credentials;
        this.configuration = conf;
        this.tableNameGenerator = tableNameGenerator;
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(conf);
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
        this.genomeHelper = new GenomeHelper(this.configuration);
        this.variantTable = credentials.getTable();
        ObjectMap options = configuration.getStorageEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID).getVariant().getOptions();
        HBaseVariantStorageMetadataDBAdaptorFactory factory = new HBaseVariantStorageMetadataDBAdaptorFactory(
                hBaseManager, tableNameGenerator.getMetaTableName(), conf);
        this.studyConfigurationManager.set(new StudyConfigurationManager(factory));
        this.variantFileMetadataDBAdaptor = factory.buildVariantFileMetadataDBAdaptor();

        clientSideSkip = !options.getBoolean(PhoenixHelper.PHOENIX_SERVER_OFFSET_AVAILABLE, true);
        this.queryParser = new VariantSqlQueryParser(genomeHelper, this.variantTable,
                studyConfigurationManager.get(), cellBaseUtils, clientSideSkip);

        phoenixFetchSize = options.getInt(HadoopVariantStorageEngine.DBADAPTOR_PHOENIX_FETCH_SIZE, -1);

        phoenixHelper = new VariantPhoenixHelper(genomeHelper);

        hbaseQueryParser = new VariantHBaseQueryParser(genomeHelper, studyConfigurationManager.get());
    }

    public java.sql.Connection getJdbcConnection() {
        if (phoenixCon.get() == null) {
            try {
                java.sql.Connection connection = phoenixHelper.newJdbcConnection(this.configuration);
                if (!phoenixCon.compareAndSet(null, connection)) {
                    close(connection); // already set in the mean time
                } else {
                    logger.info("Opened Phoenix Connection " + connection);
                }
            } catch (SQLException | ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }
        return phoenixCon.get();
    }

    public GenomeHelper getGenomeHelper() {
        return genomeHelper;
    }

    public HBaseManager getHBaseManager() {
        return hBaseManager;
    }

    public HBaseCredentials getCredentials() {
        return credentials;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Connection getConnection() {
        return hBaseManager.getConnection();
    }

    public String getVariantTable() {
        return variantTable;
    }

    public String getArchiveTableName(int studyId) {
        return tableNameGenerator.getArchiveTableName(studyId);
    }

    public HBaseVariantTableNameGenerator getTableNameGenerator() {
        return tableNameGenerator;
    }

    public static Configuration getHbaseConfiguration(Configuration configuration, HBaseCredentials credentials) {

        // HBase configuration
        configuration = HBaseManager.addHBaseSettings(configuration, credentials);

        return configuration;
    }

    public ArchiveTableHelper getArchiveHelper(int studyId, int fileId) throws StorageEngineException, IOException {
        VariantFileMetadata fileMetadata = getStudyConfigurationManager().getVariantFileMetadata(studyId, fileId, null).first();
        if (fileMetadata == null) {
            throw new StorageEngineException("File '" + fileId + "' not found in study '" + studyId + "'");
        }
        return new ArchiveTableHelper(genomeHelper, studyId, fileMetadata);

    }

    @Deprecated
    public HBaseVariantFileMetadataDBAdaptor getVariantFileMetadataDBAdaptor() {
        return variantFileMetadataDBAdaptor;
    }

    @Override
    public StudyConfigurationManager getStudyConfigurationManager() {
        return studyConfigurationManager.get();
    }

    @Override
    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager.set(studyConfigurationManager);
    }

    @Override
    public void close() throws IOException {
        this.hBaseManager.close();
        try {
           close(this.phoenixCon.getAndSet(null));
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void close(java.sql.Connection connection) throws SQLException {
        if (connection != null) {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            logger.info("Close Phoenix connection {} called from {}", connection, Arrays.toString(stackTrace));
            connection.close();
        }
    }

    public static Logger getLog() {
        return logger;
    }

    @Override
    public VariantQueryResult<Variant> get(Query query, QueryOptions options) {

        List<Variant> variants = new LinkedList<>();
        VariantDBIterator iterator = iterator(query, options);
        iterator.forEachRemaining(variants::add);
        long numTotalResults;
        String warn = "";
        String error = "";

        if (options == null) {
            numTotalResults = variants.size();
        } else {
            if (options.getInt(QueryOptions.LIMIT, -1) > 0) {
                if (options.getBoolean(QueryOptions.SKIP_COUNT, DEFAULT_SKIP_COUNT)) {
                    numTotalResults = -1;
                } else {
                    numTotalResults = count(query).first();
                }
            } else {
                // There are no limit. Do not count.
                numTotalResults = variants.size();
            }
        }

        Map<String, List<String>> samples = getSamplesMetadata(query, options, getStudyConfigurationManager());
        return new VariantQueryResult<>("getVariants", ((int) iterator.getTimeFetching()), variants.size(), numTotalResults,
                warn, error, variants, samples, HadoopVariantStorageEngine.STORAGE_ENGINE_ID);
    }

    @Override
    public List<VariantQueryResult<Variant>> get(List<Query> queries, QueryOptions options) {
        List<VariantQueryResult<Variant>> results = new ArrayList<>(queries.size());
        for (Query query : queries) {
            results.add(get(query, options));
        }
        return results;
    }

    @Override
    public VariantQueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options,
                                                 int windowsSize) {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public QueryResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options) {
        StopWatch stopWatch = StopWatch.createStarted();
        Iterator<VariantAnnotation> variantAnnotationIterator = annotationIterator(name, query, options);

        List<VariantAnnotation> annotations = new ArrayList<>();
        variantAnnotationIterator.forEachRemaining(annotations::add);

        return new QueryResult<>("getAnnotation", ((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), annotations.size(), -1,
                "", "", annotations);
    }

    public Iterator<VariantAnnotation> annotationIterator(String name, Query query, QueryOptions options) {
        query = query == null ? new Query() : new Query(query);
        options = validateAnnotationQueryOptions(options);
        validateAnnotationQuery(query);

        byte[] annotationColumn;
        if (name.equals(VariantAnnotationManager.LATEST)) {
            annotationColumn = VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes();
        } else {
            ProjectMetadata.VariantAnnotationMetadata saved = getStudyConfigurationManager().getProjectMetadata().first().
                    getAnnotation().getSaved(name);

            annotationColumn = Bytes.toBytes(VariantPhoenixHelper.getAnnotationSnapshotColumn(saved.getId()));
            query.put(ANNOT_NAME.key(), saved.getId());
        }
        SelectVariantElements selectElements = VariantQueryUtils.parseSelectElements(query, options, getStudyConfigurationManager());
        List<Scan> scans = hbaseQueryParser.parseQueryMultiRegion(selectElements, query, options);

        try {
            Table table = getConnection().getTable(TableName.valueOf(variantTable));
            Iterator<Iterator<Result>> iterators = scans.stream().map(scan -> {
                try {
                    return table.getScanner(scan).iterator();
                } catch (IOException e) {
                    throw VariantQueryException.internalException(e);
                }
            }).iterator();
            HBaseToVariantAnnotationConverter converter = new HBaseToVariantAnnotationConverter(genomeHelper)
                    .setIncludeFields(selectElements.getFields());
            converter.setAnnotationColumn(annotationColumn);
            Iterator<Result> iterator = Iterators.concat(iterators);
            int skip = options.getInt(QueryOptions.SKIP);
            if (skip > 0) {
                Iterators.advance(iterator, skip);
            }
            int limit = options.getInt(QueryOptions.LIMIT);
            if (limit > 0) {
                iterator = Iterators.limit(iterator, limit);
            }
            return Iterators.transform(iterator, converter::convert);
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) {
        if (query == null) {
            query = new Query();
        }
        long startTime = System.currentTimeMillis();
        String sql = queryParser.parse(query, new QueryOptions(QueryOptions.COUNT, true)).getSql();
        logger.info(sql);
        try (Statement statement = getJdbcConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) { // Cleans up Statement and RS
            resultSet.next();
            long count = resultSet.getLong(1);
            return new QueryResult<>("count", ((int) (System.currentTimeMillis() - startTime)),
                    1, 1, "", "", Collections.singletonList(count));
        } catch (SQLException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {

        if (options == null) {
            options = new QueryOptions();
        }

        if (query == null) {
            query = new Query();
        }

        boolean archiveIterator = options.getBoolean("archive", false);
        boolean hbaseIterator = options.getBoolean(NATIVE, false);
        // || VariantHBaseQueryParser.fullySupportedQuery(query);

        if (archiveIterator) {
            String study = query.getString(STUDY.key());
            StudyConfiguration studyConfiguration;
            int studyId;
            if (StringUtils.isNumeric(study)) {
                studyId = Integer.parseInt(study);
                studyConfiguration = getStudyConfigurationManager().getStudyConfiguration(studyId, options).first();
            } else {
                studyConfiguration = getStudyConfigurationManager().getStudyConfiguration(study, options).first();
                studyId = studyConfiguration.getStudyId();
            }

            int fileId = query.getInt(FILE.key());
            if (!studyConfiguration.getFileIds().containsValue(fileId)) {
                return VariantDBIterator.emptyIterator();
            }

            LinkedHashSet<Integer> samlpeIds = studyConfiguration.getSamplesInFiles().get(fileId);
            List<String> returnedSamples = new ArrayList<>(samlpeIds.size());
            for (Integer sampleId : samlpeIds) {
                returnedSamples.add(studyConfiguration.getSampleIds().inverse().get(sampleId));
            }
            query.put(INCLUDE_SAMPLE.key(), returnedSamples);

            Region region = null;
            if (!StringUtils.isEmpty(query.getString(REGION.key()))) {
                region = Region.parseRegion(query.getString(REGION.key()));
            }

            //Get the ArchiveHelper related with the requested file.
            ArchiveTableHelper archiveHelper;
            try {
                archiveHelper = getArchiveHelper(studyId, fileId);
            } catch (IOException | StorageEngineException e) {
                throw VariantQueryException.internalException(e);
            }

            Scan scan = new Scan();
            scan.addColumn(archiveHelper.getColumnFamily(), archiveHelper.getNonRefColumnName());
            scan.addColumn(archiveHelper.getColumnFamily(), archiveHelper.getRefColumnName());
            VariantHBaseQueryParser.addArchiveRegionFilter(scan, region, archiveHelper);
            scan.setMaxResultSize(options.getInt("limit"));
            String tableName = getTableNameGenerator().getArchiveTableName(studyId);

            logger.debug("Creating {} iterator", VariantHadoopArchiveDBIterator.class);
            logger.debug("Table name = " + tableName);
            logger.debug("StartRow = " + new String(scan.getStartRow()));
            logger.debug("StopRow = " + new String(scan.getStopRow()));
            logger.debug("MaxResultSize = " + scan.getMaxResultSize());
            logger.debug("region = " + region);
            logger.debug("Column name = " + fileId);
            logger.debug("Chunk size = " + archiveHelper.getChunkSize());

            try (Table table = getConnection().getTable(TableName.valueOf(tableName))) {
                ResultScanner resScan = table.getScanner(scan);
                return new VariantHadoopArchiveDBIterator(resScan, archiveHelper, options).setRegion(region);
            } catch (IOException e) {
                throw VariantQueryException.internalException(e);
            }
        } else if (hbaseIterator) {
            logger.debug("Creating " + VariantHBaseScanIterator.class.getSimpleName() + " iterator");
            SelectVariantElements selectElements = VariantQueryUtils.parseSelectElements(query, options, studyConfigurationManager.get());
            List<Scan> scans = hbaseQueryParser.parseQueryMultiRegion(selectElements, query, options);
            try {
                Table table = getConnection().getTable(TableName.valueOf(variantTable));
                String unknownGenotype = null;
                if (isValidParam(query, UNKNOWN_GENOTYPE)) {
                    unknownGenotype = query.getString(UNKNOWN_GENOTYPE.key());
                }
                List<String> formats = getIncludeFormats(query);
                Iterator<ResultScanner> resScans = scans.stream().map(scan -> {
                    try {
                        return table.getScanner(scan);
                    } catch (IOException e) {
                        throw VariantQueryException.internalException(e);
                    }
                }).iterator();

                VariantHBaseScanIterator iterator = new VariantHBaseScanIterator(
                        resScans, genomeHelper, studyConfigurationManager.get(), options, unknownGenotype, formats, selectElements);

                // Client side skip!
                int skip = options.getInt(QueryOptions.SKIP, -1);
                if (skip > 0) {
                    logger.info("Client side skip! skip = {}", skip);
                    iterator.skip(skip);
                }
                return iterator;
            } catch (IOException e) {
                throw VariantQueryException.internalException(e);
            }
        } else {

            logger.debug("Table name = " + variantTable);
            VariantSqlQueryParser.VariantPhoenixSQLQuery phoenixQuery = queryParser.parse(query, options);
            String sql = phoenixQuery.getSql();
            logger.info("Query : " + VariantQueryUtils.printQuery(query));
            logger.info(sql);
            logger.debug("Creating {} iterator", VariantHBaseResultSetIterator.class);
            try {
                Statement statement = getJdbcConnection().createStatement(); // Statemnet closed by iterator
                statement.setFetchSize(options.getInt("batchSize", phoenixFetchSize));
                ResultSet resultSet = statement.executeQuery(sql); // RS closed by iterator

                if (options.getBoolean("explain", false)) {
                    logger.info("---- " + "EXPLAIN " + sql);
//                    phoenixHelper.getPhoenixHelper().explain(getJdbcConnection(), sql, Logger::info);
                    List<String> planSteps = new LinkedList<>();
                    resultSet.unwrap(PhoenixResultSet.class).getUnderlyingIterator().explain(planSteps);
                    for (String planStep : planSteps) {
                        logger.info(" | " +  planStep);
                    }
                }
                List<String> formats = getIncludeFormats(query);
                String unknownGenotype = null;
                if (isValidParam(query, UNKNOWN_GENOTYPE)) {
                    unknownGenotype = query.getString(UNKNOWN_GENOTYPE.key());
                }

//                VariantPhoenixCursorIterator iterator = new VariantPhoenixCursorIterator(phoenixQuery, getJdbcConnection(), converter);
                VariantHBaseResultSetIterator iterator = new VariantHBaseResultSetIterator(statement,
                        resultSet, genomeHelper, getStudyConfigurationManager(), phoenixQuery.getSelect(),
                        formats, unknownGenotype, options);

                if (clientSideSkip) {
                    // Client side skip!
                    int skip = options.getInt(QueryOptions.SKIP, -1);
                    if (skip > 0) {
                        logger.info("Client side skip! skip = {}", skip);
                        iterator.skip(skip);
                    }
                }
                return iterator;
            } catch (SQLException e) {
                if (e.getErrorCode() == SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode()) {
                    try {
                        logger.error(e.getMessage());
                        List<PhoenixHelper.Column> columns = phoenixHelper.getPhoenixHelper()
                                .getColumns(getJdbcConnection(), variantTable, VariantPhoenixHelper.DEFAULT_TABLE_TYPE);
                        logger.info("Available columns from table " + variantTable + " :");
                        for (PhoenixHelper.Column column : columns) {
                            logger.info(" - " + column.toColumnInfo());
                        }
                    } catch (SQLException e1) {
                        logger.error("Error reading columns for table " + variantTable, e1);
                    }
                }
                throw VariantQueryException.internalException(e);
            }
        }
    }

    @Override
    public void forEach(Consumer<? super Variant> action) {
        iterator().forEachRemaining(action);
    }

    @Override
    public void forEach(Query query, Consumer<? super Variant> action, QueryOptions options) {
        iterator(query, options).forEachRemaining(action);
    }

    @Override
    public QueryResult getFrequency(Query query, Region region, int regionIntervalSize) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /**
     * Ensure that all the annotation fields exist are defined.
     *
     * @param studyConfiguration StudyConfiguration where the cohorts are defined
     * @throws SQLException is there is any error with Phoenix
     */
    public void updateStatsColumns(StudyConfiguration studyConfiguration) throws SQLException {
        phoenixHelper.updateStatsColumns(getJdbcConnection(), variantTable, studyConfiguration);
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        return updateStats(variantStatsWrappers,
                getStudyConfigurationManager().getStudyConfiguration(studyName, queryOptions).first(), queryOptions);
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration,
                                   QueryOptions options) {

        VariantStatsToHBaseConverter converter = new VariantStatsToHBaseConverter(genomeHelper, studyConfiguration);
        List<Put> puts = converter.apply(variantStatsWrappers);

        long start = System.currentTimeMillis();
        try (Table table = getConnection().getTable(TableName.valueOf(variantTable))) {
            table.put(puts);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new QueryResult<>("Update annotations", (int) (System.currentTimeMillis() - start), 0, 0, "", "", Collections.emptyList());
    }

    public VariantAnnotationPhoenixDBWriter newAnnotationLoader(QueryOptions options) {
        try {
            return new VariantAnnotationPhoenixDBWriter(this, options, variantTable,
                    phoenixHelper.newJdbcConnection(this.configuration), true);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Deprecated
    public QueryResult updateAnnotations(List<org.opencb.biodata.models.variant.avro.VariantAnnotation> variantAnnotations,
                                         QueryOptions queryOptions) {

        long start = System.currentTimeMillis();

        final GenomeHelper genomeHelper1 = new GenomeHelper(configuration);
        VariantAnnotationToPhoenixConverter converter = new VariantAnnotationToPhoenixConverter(genomeHelper1.getColumnFamily());
        Iterable<Map<PhoenixHelper.Column, ?>> records = converter.apply(variantAnnotations);

        String fullTableName = VariantPhoenixHelper.getEscapedFullTableName(variantTable, getConfiguration());
        try (java.sql.Connection conn = phoenixHelper.newJdbcConnection(this.configuration);
             VariantAnnotationUpsertExecutor upsertExecutor =
                     new VariantAnnotationUpsertExecutor(conn, fullTableName)) {
            upsertExecutor.execute(records);
            upsertExecutor.close();
            getLog().info("Phoenix connection is autoclosed ... " + conn);
        } catch (SQLException | ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
        return new QueryResult("Update annotations", (int) (System.currentTimeMillis() - start), 0, 0, "", "", Collections.emptyList());
    }

    @Override
    public QueryResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    public <T> void addNotNull(Collection<T> collection, T value) {
        if (value != null) {
            collection.add(value);
        }
    }

}
