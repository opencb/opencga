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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.iterators.VariantHBaseResultSetIterator;
import org.opencb.opencga.storage.hadoop.variant.adaptors.iterators.VariantHBaseScanIterator;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.VariantAnnotationPhoenixDBWriter;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.VariantAnnotationUpsertExecutor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHadoopArchiveDBIterator;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseFileMetadataDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_INDEX_LAST_TIMESTAMP;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

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
    private final AtomicReference<VariantStorageMetadataManager> studyConfigurationManager = new AtomicReference<>(null);
    private final Configuration configuration;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final GenomeHelper genomeHelper;
    private final AtomicReference<java.sql.Connection> phoenixCon = new AtomicReference<>();
    private final VariantSqlQueryParser queryParser;
    private final VariantHBaseQueryParser hbaseQueryParser;
    private final HBaseFileMetadataDBAdaptor variantFileMetadataDBAdaptor;
    private final int phoenixFetchSize;
    private boolean clientSideSkip;
    private HBaseManager hBaseManager;

    public VariantHadoopDBAdaptor(HBaseManager hBaseManager, HBaseCredentials credentials, StorageConfiguration configuration,
                                  Configuration conf, HBaseVariantTableNameGenerator tableNameGenerator)
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
        ObjectMap options = configuration.getVariantEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID).getOptions();
        HBaseVariantStorageMetadataDBAdaptorFactory factory = new HBaseVariantStorageMetadataDBAdaptorFactory(
                hBaseManager, tableNameGenerator.getMetaTableName(), conf);
        this.studyConfigurationManager.set(new VariantStorageMetadataManager(factory));
        this.variantFileMetadataDBAdaptor = factory.buildFileMetadataDBAdaptor();

        clientSideSkip = !options.getBoolean(PhoenixHelper.PHOENIX_SERVER_OFFSET_AVAILABLE, true);
        this.queryParser = new VariantSqlQueryParser(genomeHelper, this.variantTable,
                studyConfigurationManager.get(), clientSideSkip);

        phoenixFetchSize = options.getInt(
                HadoopVariantStorageOptions.DBADAPTOR_PHOENIX_FETCH_SIZE.key(),
                HadoopVariantStorageOptions.DBADAPTOR_PHOENIX_FETCH_SIZE.defaultValue());

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

    public ArchiveTableHelper getArchiveHelper(int studyId, int fileId) throws StorageEngineException {
        VariantFileMetadata fileMetadata = getMetadataManager().getVariantFileMetadata(studyId, fileId, null).first();
        if (fileMetadata == null) {
            throw VariantQueryException.fileNotFound(fileId, studyId);
        }
        return new ArchiveTableHelper(genomeHelper, studyId, fileMetadata);

    }

    @Deprecated
    public HBaseFileMetadataDBAdaptor getVariantFileMetadataDBAdaptor() {
        return variantFileMetadataDBAdaptor;
    }

    @Override
    public VariantStorageMetadataManager getMetadataManager() {
        return studyConfigurationManager.get();
    }

    @Override
    public void setVariantStorageMetadataManager(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.studyConfigurationManager.set(variantStorageMetadataManager);
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
    public VariantQueryResult<Variant> get(ParsedVariantQuery query, QueryOptions options) {

        List<Variant> variants = new LinkedList<>();
        VariantDBIterator iterator = iterator(query, options);
        iterator.forEachRemaining(variants::add);
        long numTotalResults;

        if (options == null) {
            numTotalResults = variants.size();
        } else {
            if (options.getInt(QueryOptions.LIMIT, -1) >= 0) {
                if (options.getBoolean(QueryOptions.COUNT, false)) {
                    numTotalResults = count(query).first();
                } else {
                    numTotalResults = -1;
                }
            } else {
                // There are no limit. Do not count.
                numTotalResults = variants.size();
            }
        }

        VariantQueryResult<Variant> result = new VariantQueryResult<>(((int) iterator.getTimeFetching()), variants.size(),
                numTotalResults, null, variants, null, HadoopVariantStorageEngine.STORAGE_ENGINE_ID);
        return addSamplesMetadataIfRequested(result, query.getQuery(), options, getMetadataManager());
    }

    @Override
    public VariantQueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options,
                                                 int windowsSize) {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public DataResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options) {
        StopWatch stopWatch = StopWatch.createStarted();
        Iterator<VariantAnnotation> variantAnnotationIterator = annotationIterator(name, query, options);

        List<VariantAnnotation> annotations = new ArrayList<>();
        variantAnnotationIterator.forEachRemaining(annotations::add);

        return new DataResult<>(((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), Collections.emptyList(), annotations.size(), annotations,
                -1);
    }

    public Iterator<VariantAnnotation> annotationIterator(String name, Query query, QueryOptions options) {
        query = query == null ? new Query() : new Query(query);
        options = validateAnnotationQueryOptions(options);
        validateAnnotationQuery(query);

        byte[] annotationColumn;
        if (name.equals(VariantAnnotationManager.CURRENT)) {
            annotationColumn = VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes();
        } else {
            ProjectMetadata.VariantAnnotationMetadata saved = getMetadataManager().getProjectMetadata().
                    getAnnotation().getSaved(name);

            annotationColumn = Bytes.toBytes(VariantPhoenixHelper.getAnnotationSnapshotColumn(saved.getId()));
            query.put(ANNOT_NAME.key(), saved.getId());
        }
        VariantQueryProjection selectElements = VariantQueryProjectionParser.parseVariantQueryFields(query, options, getMetadataManager());
        List<Scan> scans = hbaseQueryParser.parseQueryMultiRegion(selectElements, query, options);

        Iterator<Iterator<Result>> iterators = scans.stream().map(scan -> {
            try {
                return hBaseManager.getScanner(variantTable, scan).iterator();
            } catch (IOException e) {
                throw VariantQueryException.internalException(e);
            }
        }).iterator();
        long ts = getMetadataManager().getProjectMetadata().getAttributes()
                .getLong(SEARCH_INDEX_LAST_TIMESTAMP.key());
        HBaseToVariantAnnotationConverter converter = new HBaseToVariantAnnotationConverter(ts)
                .setAnnotationIds(getMetadataManager().getProjectMetadata().getAnnotation())
                .setIncludeFields(selectElements.getFields());
        converter.setAnnotationColumn(annotationColumn, name);
        Iterator<Result> iterator = Iterators.concat(iterators);
        int skip = options.getInt(QueryOptions.SKIP);
        if (skip > 0) {
            Iterators.advance(iterator, skip);
        }
        int limit = options.getInt(QueryOptions.LIMIT);
        if (limit >= 0) {
            iterator = Iterators.limit(iterator, limit);
        }
        return Iterators.transform(iterator, converter::convert);
    }

    @Override
    public DataResult<Long> count(ParsedVariantQuery query) {
        long startTime = System.currentTimeMillis();
        String sql = queryParser.parse(query, new QueryOptions(QueryOptions.COUNT, true));
        logger.info(sql);
        try (Statement statement = getJdbcConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) { // Cleans up Statement and RS
            resultSet.next();
            long count = resultSet.getLong(1);
            return new DataResult<>(((int) (System.currentTimeMillis() - startTime)), Collections.emptyList(),
                    0, Collections.singletonList(count), count);
        } catch (SQLException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    public DataResult distinct(Query query, String field) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public VariantDBIterator iterator(ParsedVariantQuery variantQuery, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }

        boolean archiveIterator = options.getBoolean("archive", false);
        boolean hbaseIterator = options.getBoolean(NATIVE, VariantHBaseQueryParser.isSupportedQuery(variantQuery.getQuery()));
        // || VariantHBaseQueryParser.fullySupportedQuery(query);

        if (archiveIterator) {
            return archiveIterator(variantQuery, options);
        }

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        Query query = variantQuery.getQuery();
        String unknownGenotype = null;
        if (isValidParam(query, UNKNOWN_GENOTYPE)) {
            unknownGenotype = query.getString(UNKNOWN_GENOTYPE.key());
        }
        List<String> formats = getIncludeFormats(query);

        HBaseVariantConverterConfiguration converterConfiguration = HBaseVariantConverterConfiguration.builder()
                .setMutableSamplesPosition(false)
                .setStudyNameAsStudyId(options.getBoolean(HBaseVariantConverterConfiguration.STUDY_NAME_AS_STUDY_ID, true))
                .setSimpleGenotypes(options.getBoolean(HBaseVariantConverterConfiguration.SIMPLE_GENOTYPES, true))
                .setUnknownGenotype(unknownGenotype)
                .setProjection(variantQuery.getProjection())
                .setFormat(formats)
                .setIncludeSampleId(query.getBoolean(INCLUDE_SAMPLE_ID.key(), false))
                .setIncludeIndexStatus(query.getBoolean(VariantQueryUtils.VARIANTS_TO_INDEX.key(), false))
                .build();

        if (hbaseIterator) {
            logger.debug("Creating " + VariantHBaseScanIterator.class.getSimpleName() + " iterator");
            List<Scan> scans = hbaseQueryParser.parseQueryMultiRegion(variantQuery, options);
            Iterator<ResultScanner> resScans = scans.stream().map(scan -> {
                try {
                    return hBaseManager.getScanner(variantTable, scan);
                } catch (IOException e) {
                    throw VariantQueryException.internalException(e);
                }
            }).iterator();

            VariantHBaseScanIterator iterator = new VariantHBaseScanIterator(
                    resScans, metadataManager, converterConfiguration, options);

            // Client side skip!
            int skip = options.getInt(QueryOptions.SKIP, -1);
            if (skip > 0) {
                logger.info("Client side skip! skip = {}", skip);
                iterator.skip(skip);
            }
            return iterator;
        } else {
            logger.debug("Table name = " + variantTable);
            logger.info("Query : " + VariantQueryUtils.printQuery(query));
            String sql = queryParser.parse(variantQuery, options);
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

//                VariantPhoenixCursorIterator iterator = new VariantPhoenixCursorIterator(phoenixQuery, getJdbcConnection(), converter);
                VariantHBaseResultSetIterator iterator = new VariantHBaseResultSetIterator(statement,
                        resultSet, metadataManager, converterConfiguration);

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

    private VariantDBIterator archiveIterator(ParsedVariantQuery variantQuery, QueryOptions options) {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        Query query = variantQuery.getQuery();
        String study = query.getString(STUDY.key());
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        int studyId = studyMetadata.getId();

        String file = query.getString(FILE.key());
        Integer fileId = metadataManager.getFileId(studyMetadata.getId(), file, true);
        if (fileId == null) {
            throw VariantQueryException.fileNotFound(file, study);
        }
        LinkedHashSet<Integer> sampleIds = metadataManager.getFileMetadata(studyId, fileId).getSamples();
        query.put(INCLUDE_SAMPLE.key(), new ArrayList<>(sampleIds));

        Region region = null;
        if (!StringUtils.isEmpty(query.getString(REGION.key()))) {
            region = Region.parseRegion(query.getString(REGION.key()));
        }

        //Get the ArchiveHelper related with the requested file.
        ArchiveTableHelper archiveHelper;
        try {
            archiveHelper = getArchiveHelper(studyId, fileId);
        } catch (StorageEngineException e) {
            throw VariantQueryException.internalException(e);
        }

        Scan scan = new Scan();
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, archiveHelper.getNonRefColumnName());
        if (options.getBoolean("ref", true)) {
            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, archiveHelper.getRefColumnName());
        }
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
    public DataResult getFrequency(Query query, Region region, int regionIntervalSize) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public DataResult rank(Query query, String field, int numResults, boolean asc) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /**
     * Ensure that all the annotation fields exist are defined.
     *
     * @param studyMetadata StudyMetadata where the cohorts are defined
     * @throws SQLException is there is any error with Phoenix
     */
    public void updateStatsColumns(StudyMetadata studyMetadata) throws SQLException {
        List<Integer> cohortIds = new ArrayList<>();
        getMetadataManager().cohortIterator(studyMetadata.getId())
                .forEachRemaining(cohortMetadata -> {
                    if (cohortMetadata.isStatsReady()) {
                        cohortIds.add(cohortMetadata.getId());
                    }
                });
        phoenixHelper.updateStatsColumns(getJdbcConnection(), variantTable, studyMetadata.getId(), cohortIds);
    }

    /**
     * @deprecated This method should not be used for batch load.
     */
    @Override
    @Deprecated
    public DataResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, long timestamp,
                                   QueryOptions queryOptions) {
        throw new UnsupportedOperationException("Unimplemented method");
    }

    /**
     * @deprecated This method should not be used for batch load.
     */
    @Override
    @Deprecated
    public DataResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyMetadata studyMetadata,
                                   long timestamp, QueryOptions options) {
        throw new UnsupportedOperationException("Unimplemented method");
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
    public DataResult updateAnnotations(List<VariantAnnotation> variantAnnotations,
                                         long timestamp, QueryOptions queryOptions) {

        long start = System.currentTimeMillis();

        final GenomeHelper genomeHelper1 = new GenomeHelper(configuration);
        int currentAnnotationId = getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getId();
        VariantAnnotationToPhoenixConverter converter = new VariantAnnotationToPhoenixConverter(GenomeHelper.COLUMN_FAMILY_BYTES,
                currentAnnotationId);
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
        return new DataResult((int) (System.currentTimeMillis() - start), Collections.emptyList(), 0, variantAnnotations.size(), 0, 0);
    }

    @Override
    public DataResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, long timeStamp,
                                               QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    public <T> void addNotNull(Collection<T> collection, T value) {
        if (value != null) {
            collection.add(value);
        }
    }

}
