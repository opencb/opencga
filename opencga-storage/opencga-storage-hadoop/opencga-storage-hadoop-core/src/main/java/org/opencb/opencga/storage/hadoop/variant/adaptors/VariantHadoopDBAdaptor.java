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
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.MultiVariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
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
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
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
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_INDEX_LAST_TIMESTAMP;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;


public class VariantHadoopDBAdaptor implements VariantDBAdaptor {
    public static final String NATIVE = "native";
    public static final QueryParam ANNOT_NAME = QueryParam.create("annotName", "", Type.TEXT);

    protected static Logger logger = LoggerFactory.getLogger(VariantHadoopDBAdaptor.class);
    private final String variantTable;
    private final PhoenixHelper phoenixHelper;
    private final AtomicReference<VariantStorageMetadataManager> studyConfigurationManager = new AtomicReference<>(null);
    private final Configuration configuration;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final VariantSqlQueryParser queryParser;
    private final VariantHBaseQueryParser hbaseQueryParser;
    private final HBaseFileMetadataDBAdaptor variantFileMetadataDBAdaptor;
    private final int phoenixFetchSize;
    private final int phoenixQueryComplexityThreshold;
    private boolean clientSideSkip;
    private HBaseManager hBaseManager;

    public VariantHadoopDBAdaptor(HBaseManager hBaseManager, StorageConfiguration configuration,
                                  Configuration conf, HBaseVariantTableNameGenerator tableNameGenerator)
            throws IOException {
        this(hBaseManager, conf, tableNameGenerator,
                configuration.getVariantEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID).getOptions());
    }

    public VariantHadoopDBAdaptor(HBaseManager hBaseManager,
                                  Configuration conf, HBaseVariantTableNameGenerator tableNameGenerator, ObjectMap options)
            throws IOException {
        this.configuration = conf;
        this.tableNameGenerator = tableNameGenerator;
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(conf);
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
        this.variantTable = tableNameGenerator.getVariantTableName();

        HBaseVariantStorageMetadataDBAdaptorFactory factory = new HBaseVariantStorageMetadataDBAdaptorFactory(
                hBaseManager, tableNameGenerator.getMetaTableName(), conf);
        this.studyConfigurationManager.set(new VariantStorageMetadataManager(factory));
        this.variantFileMetadataDBAdaptor = factory.buildFileMetadataDBAdaptor();

        clientSideSkip = !options.getBoolean(PhoenixHelper.PHOENIX_SERVER_OFFSET_AVAILABLE, true);
        this.queryParser = new VariantSqlQueryParser(this.variantTable,
                studyConfigurationManager.get(), clientSideSkip, this.configuration);

        phoenixFetchSize = options.getInt(
                HadoopVariantStorageOptions.DBADAPTOR_PHOENIX_FETCH_SIZE.key(),
                HadoopVariantStorageOptions.DBADAPTOR_PHOENIX_FETCH_SIZE.defaultValue());

        phoenixQueryComplexityThreshold = options.getInt(
                HadoopVariantStorageOptions.DBADAPTOR_PHOENIX_QUERY_COMPLEXITY_THRESHOLD.key(),
                HadoopVariantStorageOptions.DBADAPTOR_PHOENIX_QUERY_COMPLEXITY_THRESHOLD.defaultValue());

        phoenixHelper = new PhoenixHelper(this.configuration);

        hbaseQueryParser = new VariantHBaseQueryParser(studyConfigurationManager.get());
    }

    public java.sql.Connection openJdbcConnection() {
        // Do not pool Phoenix connections. These are lightweight. Pooling might cause issues.
        // See : https://phoenix.apache.org/faq.html#Should_I_pool_Phoenix_JDBC_Connections
        try {
            return phoenixHelper.openJdbcConnection();
        } catch (SQLException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public HBaseManager getHBaseManager() {
        return hBaseManager;
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
        return new ArchiveTableHelper(configuration, studyId, fileMetadata);

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

        VariantQueryResult<Variant> result = new VariantQueryResult<>(iterator.getTime(TimeUnit.MILLISECONDS), variants.size(),
                numTotalResults, null, variants, null, HadoopVariantStorageEngine.STORAGE_ENGINE_ID)
                .setFetchTime(iterator.getTimeFetching(TimeUnit.MILLISECONDS))
                .setConvertTime(iterator.getTimeConverting(TimeUnit.MILLISECONDS));
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
            annotationColumn = VariantPhoenixSchema.VariantColumn.FULL_ANNOTATION.bytes();
        } else {
            ProjectMetadata.VariantAnnotationMetadata saved = getMetadataManager().getProjectMetadata().
                    getAnnotation().getSaved(name);

            annotationColumn = Bytes.toBytes(VariantPhoenixSchema.getAnnotationSnapshotColumn(saved.getId()));
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
        try (java.sql.Connection connection = openJdbcConnection(); Statement statement = connection.createStatement();
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
        } else {
            options = new QueryOptions(options);
            // Do not modify input options
            // ignore count when creating an iterator.
            options.put(QueryOptions.COUNT, false);
        }

        boolean archiveIterator = options.getBoolean("archive", false);
        Set<String> unsupportedParamsFromQuery = VariantHBaseQueryParser.unsupportedParamsFromQuery(variantQuery.getQuery());
        boolean nativeSupportedQuery = unsupportedParamsFromQuery.isEmpty();
        if (!nativeSupportedQuery) {
            logger.info("Unsupported native query : " + unsupportedParamsFromQuery);
        }
        boolean hbaseIterator = nativeSupportedQuery && options.getBoolean(NATIVE, nativeSupportedQuery);
        // || VariantHBaseQueryParser.fullySupportedQuery(query);

        if (archiveIterator) {
            return archiveIterator(variantQuery, options);
        }

        Query query = variantQuery.getQuery();
        String unknownGenotype = null;
        if (isValidParam(query, UNKNOWN_GENOTYPE)) {
            unknownGenotype = query.getString(UNKNOWN_GENOTYPE.key());
        }
        List<String> formats = getIncludeSampleData(query);

        HBaseVariantConverterConfiguration converterConfiguration = HBaseVariantConverterConfiguration.builder()
                .setMutableSamplesPosition(false)
                .setStudyNameAsStudyId(options.getBoolean(HBaseVariantConverterConfiguration.STUDY_NAME_AS_STUDY_ID, true))
                .setSimpleGenotypes(options.getBoolean(HBaseVariantConverterConfiguration.SIMPLE_GENOTYPES, true))
                .setUnknownGenotype(unknownGenotype)
                .setProjection(variantQuery.getProjection())
                .setSampleDataKeys(formats)
                .setIncludeSampleId(query.getBoolean(INCLUDE_SAMPLE_ID.key(), false))
                .setIncludeIndexStatus(query.getBoolean(VariantQueryUtils.VARIANTS_TO_INDEX.key(), false))
                .build();

        if (hbaseIterator) {
            return hbaseIterator(variantQuery, options, converterConfiguration);
        } else {
            return phoenixIterator(variantQuery, options, converterConfiguration);
        }
    }

    private VariantHBaseResultSetIterator phoenixIterator(ParsedVariantQuery variantQuery, QueryOptions options,
                                                          HBaseVariantConverterConfiguration converterConfiguration) {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        new VariantQueryParser(null, metadataManager).optimize(variantQuery);

        logger.debug("Table name = " + variantTable);
        logger.info("Query : " + VariantQueryUtils.printQuery(variantQuery.getQuery()));
        String sql = queryParser.parse(variantQuery, options);
        logger.info(sql);
        logger.debug("Creating {} iterator", VariantHBaseResultSetIterator.class);
        java.sql.Connection jdbcConnection = openJdbcConnection(); // Closed by iterator or catch
        Statement statement = null; // Closed by iterator or catch
        ResultSet resultSet = null; // Closed by iterator or catch
        try {
            statement = jdbcConnection.createStatement();
            statement.setFetchSize(options.getInt("batchSize", phoenixFetchSize));
            resultSet = statement.executeQuery(sql);

            if (options.getBoolean("explain", false)) {
                logger.info("---- " + "EXPLAIN " + sql);
//                    phoenixHelper.getPhoenixHelper().explain(getJdbcConnection(), sql, Logger::info);
                List<String> planSteps = new LinkedList<>();
                resultSet.unwrap(PhoenixResultSet.class).getUnderlyingIterator().explain(planSteps);
                for (String planStep : planSteps) {
                    logger.info(" | " + planStep);
                }
            }

//                VariantPhoenixCursorIterator iterator = new VariantPhoenixCursorIterator(phoenixQuery, getJdbcConnection(), converter);
            VariantHBaseResultSetIterator iterator = new VariantHBaseResultSetIterator(jdbcConnection, statement,
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
                    List<PhoenixHelper.Column> columns = phoenixHelper
                            .getColumns(jdbcConnection, variantTable, VariantPhoenixSchema.DEFAULT_TABLE_TYPE);
                    logger.info("Available columns from table " + variantTable + " :");
                    for (PhoenixHelper.Column column : columns) {
                        logger.info(" - " + column.toColumnInfo());
                    }
                } catch (SQLException e1) {
                    logger.error("Error reading columns for table " + variantTable, e1);
                }
            }
            closeOrSuppress(jdbcConnection, e);
            closeOrSuppress(statement, e);
            closeOrSuppress(resultSet, e);
            throw VariantQueryException.internalException(e);
        } catch (Exception e) {
            closeOrSuppress(jdbcConnection, e);
            closeOrSuppress(statement, e);
            closeOrSuppress(resultSet, e);
            throw e;
        }
    }

    private VariantHBaseScanIterator hbaseIterator(ParsedVariantQuery variantQuery, QueryOptions options,
                                                   HBaseVariantConverterConfiguration converterConfiguration) {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
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
    }

    private void closeOrSuppress(AutoCloseable autoCloseable, Exception e) {
        if (autoCloseable != null) {
            try {
                autoCloseable.close();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
        }
    }

    private VariantDBIterator archiveIterator(ParsedVariantQuery variantQuery, QueryOptions options) {
        Query query = variantQuery.getQuery();
        return archiveIterator(query.getString(STUDY.key()), query.getString(FILE.key()), query, options);
    }

    public VariantDBIterator archiveIterator(String study, String file, Query query, QueryOptions options) {
        VariantStorageMetadataManager metadataManager = getMetadataManager();

        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        int studyId = studyMetadata.getId();

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
        scan.setMaxResultSize(options.getInt(QueryOptions.LIMIT, -1));
        String tableName = getTableNameGenerator().getArchiveTableName(studyId);

        logger.info("Creating {} iterator", VariantHadoopArchiveDBIterator.class);
        logger.info("Table name = " + tableName);
        logger.info("StartRow = " + Bytes.toStringBinary(scan.getStartRow()));
        logger.info("StopRow = " + Bytes.toStringBinary(scan.getStopRow()));
        logger.info("MaxResultSize = " + scan.getMaxResultSize());
        logger.info("region = " + region);
        logger.info("Column name = " + scan.getFamilyMap().getOrDefault(GenomeHelper.COLUMN_FAMILY_BYTES, Collections.emptyNavigableSet())
                .stream().map(Bytes::toString).collect(Collectors.joining(",")));
        logger.info("Chunk size = " + archiveHelper.getChunkSize());

        try (Table table = getConnection().getTable(TableName.valueOf(tableName))) {
            ResultScanner resScan = table.getScanner(scan);
            return new VariantHadoopArchiveDBIterator(resScan, archiveHelper, options).setRegion(region);
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    public MultiVariantDBIterator iterator(Iterator<?> variants, Query query, QueryOptions options, int batchSize) {
        boolean nativeQuery = VariantHBaseQueryParser.isSupportedQuery(query);
        if (nativeQuery) {
            // Don't use custom split
            return VariantDBAdaptor.super.iterator(variants, query, options, batchSize);
        } else {
            // Use phoenix custom split for large number of genes.
            MultiVariantDBIterator.VariantQueryIterator queryIterator =
                    new VariantQueryIteratorCustomSplit(variants, query, batchSize, options);
            return new MultiVariantDBIterator(variants, options, this::iterator, queryIterator);
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
    public DataResult getFrequency(ParsedVariantQuery query, Region region, int regionIntervalSize) {
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
     * @throws StorageEngineException is there is any error with Phoenix
     */
    public void updateStatsColumns(StudyMetadata studyMetadata) throws StorageEngineException {
        List<Integer> cohortIds = new ArrayList<>();
        getMetadataManager().cohortIterator(studyMetadata.getId())
                .forEachRemaining(cohortMetadata -> {
                    if (cohortMetadata.isStatsReady()) {
                        cohortIds.add(cohortMetadata.getId());
                    }
                });
        try (VariantPhoenixSchemaManager schemaManager = new VariantPhoenixSchemaManager(this)) {
            schemaManager.registerNewCohorts(studyMetadata.getId(), cohortIds);
        } catch (SQLException e) {
            throw new StorageEngineException("Error closing schema manager", e);
        }
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
        return new VariantAnnotationPhoenixDBWriter(this, options, variantTable, openJdbcConnection(), true);
    }

    @Override
    @Deprecated
    public DataResult updateAnnotations(List<VariantAnnotation> variantAnnotations,
                                        long timestamp, QueryOptions queryOptions) {

        long start = System.currentTimeMillis();

        int currentAnnotationId = getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getId();
        VariantAnnotationToPhoenixConverter converter = new VariantAnnotationToPhoenixConverter(GenomeHelper.COLUMN_FAMILY_BYTES,
                currentAnnotationId);
        Iterable<Map<PhoenixHelper.Column, ?>> records = converter.apply(variantAnnotations);

        String fullTableName = VariantPhoenixSchema.getEscapedFullTableName(variantTable, getConfiguration());
        try (java.sql.Connection conn = openJdbcConnection();
             VariantAnnotationUpsertExecutor upsertExecutor =
                     new VariantAnnotationUpsertExecutor(conn, fullTableName)) {
            upsertExecutor.execute(records);
            upsertExecutor.close();
            logger.info("Phoenix connection is autoclosed ... " + conn);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        return new DataResult((int) (System.currentTimeMillis() - start), Collections.emptyList(), 0, variantAnnotations.size(), 0, 0, 0);
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

    private class VariantQueryIteratorCustomSplit extends MultiVariantDBIterator.VariantQueryIterator {
        private final VariantQueryParser parser;
        private final ParsedVariantQuery variantQuery;
        private final int cts;
        private final int bts;
        private final int flags;

        VariantQueryIteratorCustomSplit(Iterator<?> variants, Query query, int batchSize, QueryOptions options) {
            super(variants, query, batchSize);
            parser = new VariantQueryParser(null, getMetadataManager());
            variantQuery = parser.parseQuery(query, options);
            cts = sizeOrOne(variantQuery.getConsequenceTypes());
            bts = sizeOrOne(variantQuery.getBiotypes());
            flags = sizeOrOne(variantQuery.getTranscriptFlags());
        }

        private int sizeOrOne(List<String> list) {
            int size = list.size();
            return size == 0 ? 1 : size;
        }

        @Override
        protected boolean isTooComplex(List<Object> variants) {
            if (variants.size() < 50) {
                // Skip first 50 variants check
                return false;
            } else if (variants.size() % 10 != 0) {
                // Only check one every 10 variants
                return false;
            }
            ParsedVariantQuery variantQuery = new ParsedVariantQuery(this.variantQuery);
            variantQuery.getQuery().put(ID_INTERSECT.key(), variants);
            parser.optimize(variantQuery, true);

            int complexityIndex = 0;

            ParsedVariantQuery.VariantQueryXref xrefs = variantQuery.getXrefs();
            complexityIndex += xrefs.getGenes().size() * cts * bts * flags;
            if (complexityIndex > phoenixQueryComplexityThreshold) {
                // It is too complex
                logger.info("Limit query to {} variants due to complexity index of {}", variants.size(), complexityIndex);
                return true;
            }
            return false;
        }
    }
}
