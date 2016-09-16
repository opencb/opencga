package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.config.CellBaseConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HBaseStudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHadoopArchiveDBIterator;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseResultSetIterator;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.VariantAnnotationToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.stats.VariantStatsToHBaseConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.BIOTYPE;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.GENES;

/**
 * Created by mh719 on 16/06/15.
 */
public class VariantHadoopDBAdaptor implements VariantDBAdaptor {
    protected static Logger logger = LoggerFactory.getLogger(VariantHadoopDBAdaptor.class);
    private final String variantTable;
    private final VariantPhoenixHelper phoenixHelper;
    private final HBaseCredentials credentials;
    private final AtomicReference<StudyConfigurationManager> studyConfigurationManager = new AtomicReference<>(null);
    private final Configuration configuration;
    private final GenomeHelper genomeHelper;
    private final AtomicReference<java.sql.Connection> phoenixCon = new AtomicReference<>();
    private final VariantSqlQueryParser queryParser;
    private final HadoopVariantSourceDBAdaptor variantSourceDBAdaptor;
    private final CellBaseClient cellBaseClient;

    public VariantHadoopDBAdaptor(HBaseCredentials credentials, StorageConfiguration configuration,
                                  Configuration conf) throws IOException {
        this(null, credentials, configuration, getHbaseConfiguration(conf, credentials));
    }

    public VariantHadoopDBAdaptor(Connection connection, HBaseCredentials credentials, StorageConfiguration configuration,
                                  Configuration conf) throws IOException {
        this.credentials = credentials;
        this.configuration = conf;
        this.genomeHelper = new GenomeHelper(this.configuration, connection);
        this.variantTable = credentials.getTable();
        StorageEngineConfiguration storageEngine = configuration.getStorageEngine(HadoopVariantStorageManager.STORAGE_ENGINE_ID);
        this.studyConfigurationManager.set(
                new HBaseStudyConfigurationManager(genomeHelper, credentials.getTable(), conf, storageEngine.getVariant().getOptions()));
        this.variantSourceDBAdaptor = new HadoopVariantSourceDBAdaptor(this.genomeHelper);

        CellBaseConfiguration cellbaseConfiguration = configuration.getCellbase();
        cellBaseClient = new CellBaseClient(cellbaseConfiguration.toClientConfiguration());

        this.queryParser = new VariantSqlQueryParser(genomeHelper, this.variantTable, new VariantDBAdaptorUtils(this), cellBaseClient);

        phoenixHelper = new VariantPhoenixHelper(genomeHelper);
    }

    public java.sql.Connection getJdbcConnection() {
        if (phoenixCon.get() == null) {
            try {
                java.sql.Connection connection = phoenixHelper.newJdbcConnection(this.configuration);
                if (!phoenixCon.compareAndSet(null, connection)) {
                    connection.close(); // already set in the mean time
                }
                logger.info("Opened Phoenix Connection " + connection);
            } catch (SQLException | ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }
        return phoenixCon.get();
    }

    public GenomeHelper getGenomeHelper() {
        return genomeHelper;
    }

    public HBaseCredentials getCredentials() {
        return credentials;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static Configuration getHbaseConfiguration(Configuration configuration, HBaseCredentials credentials) {

        // HBase configuration
        configuration = HBaseConfiguration.create(configuration);
        configuration = HBaseManager.addHBaseSettings(configuration, credentials);

        return configuration;
    }

    public ArchiveHelper getArchiveHelper(int studyId, int fileId) throws StorageManagerException, IOException {
        VcfMeta vcfMeta = getVcfMeta(studyId, fileId, null);
        if (vcfMeta == null) {
            throw new StorageManagerException("File '" + fileId + "' not found in study '" + studyId + "'");
        }
        return new ArchiveHelper(genomeHelper, vcfMeta);

    }

    public VcfMeta getVcfMeta(int studyId, int fileId, QueryOptions options) throws IOException {
        HadoopVariantSourceDBAdaptor manager = getVariantSourceDBAdaptor();
        return manager.getVcfMeta(studyId, fileId, options);
    }

    @Override
    public HadoopVariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return variantSourceDBAdaptor;
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
    public CellBaseClient getCellBaseClient() {
        return cellBaseClient;
    }

    @Override
    public VariantDBAdaptorUtils getDBAdaptorUtils() {
        return queryParser.getUtils();
    }

    @Override
    public void close() throws IOException {
        this.genomeHelper.close();
        java.sql.Connection connection = this.phoenixCon.getAndSet(null);
        if (connection != null) {
            try {
                logger.info("Close Phoenix Connection " + connection);
                connection.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

    public static Logger getLog() {
        return logger;
    }

    @Override
    public void setDataWriter(DataWriter dataWriter) {
        // TODO Auto-generated method stub

    }

    @Override
    public QueryResult insert(List<Variant> variants, String studyName, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult delete(Query query, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteSamples(String studyName, List<String> sampleNames, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteFile(String studyName, String fileName, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteStudy(String studyName, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult<Variant> get(Query query, QueryOptions options) {

        List<Variant> variants = new LinkedList<>();
        VariantDBIterator iterator = iterator(query, options);
        iterator.forEachRemaining(variants::add);
        long numTotalResults;

        if (options == null) {
            numTotalResults = variants.size();
        } else {
            if (options.getInt(QueryOptions.LIMIT, -1) > 0) {
                if (options.getBoolean(QueryOptions.SKIP_COUNT, true)) {
                    numTotalResults = -1;
                } else {
                    numTotalResults = count(query).first();
                }
            } else {
                // There are no limit. Do not count.
                numTotalResults = variants.size();
            }
        }

        return new QueryResult<>("getVariants", ((int) iterator.getTimeFetching()), variants.size(), numTotalResults, "", "", variants);
    }

    @Override
    public List<QueryResult<Variant>> get(List<Query> queries, QueryOptions options) {
        List<QueryResult<Variant>> results = new ArrayList<>(queries.size());
        for (Query query : queries) {
            results.add(get(query, options));
        }
        return results;
    }

    @Override
    public QueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize) {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public QueryResult<Long> count(Query query) {
        if (query == null) {
            query = new Query();
        }
        long startTime = System.currentTimeMillis();
        String sql = queryParser.parse(query, new QueryOptions(VariantSqlQueryParser.COUNT, true));
        logger.info(sql);
        try {
            Statement statement = getJdbcConnection().createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.next();
            long count = resultSet.getLong(1);
            return new QueryResult<>("count", ((int) (System.currentTimeMillis() - startTime)),
                    1, 1, "", "", Collections.singletonList(count));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public VariantDBIterator iterator() {
        return iterator(new Query(), new QueryOptions());
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {

        if (options == null) {
            options = new QueryOptions();
        }

        if (query == null) {
            query = new Query();
        }

        if (options.getBoolean("archive", false)) {
            String study = query.getString(STUDIES.key());
            StudyConfiguration studyConfiguration;
            int studyId;
            if (StringUtils.isNumeric(study)) {
                studyId = Integer.parseInt(study);
                studyConfiguration = getStudyConfigurationManager().getStudyConfiguration(studyId, options).first();
            } else {
                studyConfiguration = getStudyConfigurationManager().getStudyConfiguration(study, options).first();
                studyId = studyConfiguration.getStudyId();
            }

            int fileId = query.getInt(FILES.key());
            if (!studyConfiguration.getFileIds().containsValue(fileId)) {
                return VariantDBIterator.emptyIterator();
            }

            LinkedHashSet<Integer> samlpeIds = studyConfiguration.getSamplesInFiles().get(fileId);
            List<String> returnedSamples = new ArrayList<>(samlpeIds.size());
            for (Integer sampleId : samlpeIds) {
                returnedSamples.add(studyConfiguration.getSampleIds().inverse().get(sampleId));
            }
            query.put(RETURNED_SAMPLES.key(), returnedSamples);

            Region region = null;
            if (!StringUtils.isEmpty(query.getString(REGION.key()))) {
                region = Region.parseRegion(query.getString(REGION.key()));
            }

            //Get the ArchiveHelper related with the requested file.
            ArchiveHelper archiveHelper;
            try {
                archiveHelper = getArchiveHelper(studyId, fileId);
            } catch (IOException | StorageManagerException e) {
                throw new RuntimeException(e);
            }

            Scan scan = new Scan();
            scan.addColumn(archiveHelper.getColumnFamily(), Bytes.toBytes(ArchiveHelper.getColumnName(fileId)));
            addArchiveRegionFilter(scan, region, archiveHelper);
            scan.setMaxResultSize(options.getInt("limit"));
            String tableName = HadoopVariantStorageManager.getArchiveTableName(studyId, genomeHelper.getConf());

            logger.debug("Creating {} iterator", VariantHadoopArchiveDBIterator.class);
            logger.debug("Table name = " + tableName);
            logger.debug("StartRow = " + new String(scan.getStartRow()));
            logger.debug("StopRow = " + new String(scan.getStopRow()));
            logger.debug("MaxResultSize = " + scan.getMaxResultSize());
            logger.debug("region = " + region);
            logger.debug("Column name = " + fileId);
            logger.debug("Chunk size = " + archiveHelper.getChunkSize());

            try (Table table = getConnection().getTable(TableName.valueOf(tableName));) {
                ResultScanner resScan = table.getScanner(scan);
                return new VariantHadoopArchiveDBIterator(resScan, archiveHelper, options).setRegion(region);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            logger.debug("Table name = " + variantTable);
            String sql = queryParser.parse(query, options);
            logger.info(sql);

//            try (Statement statement = phoenixCon.createStatement()) {
//                ResultSet resultSet = statement.executeQuery(sql);
//                return new VariantHBaseResultIterator(resultSet, genomeHelper, studyConfigurationManager, options);
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }

            logger.debug("Creating {} iterator", VariantHBaseResultSetIterator.class);
            try {
                Statement statement = getJdbcConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                List<String> returnedSamples = getDBAdaptorUtils().getReturnedSamples(query, options);
                return new VariantHBaseResultSetIterator(resultSet, genomeHelper, getStudyConfigurationManager(), options, returnedSamples);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

//            logger.debug("Creating {} iterator", VariantHBaseScanIterator.class);
//            Scan scan = parseQuery(query, options);
//            try {
//                Table table = hbaseCon.getTable(TableName.valueOf(variantTable));
//                ResultScanner resScan = table.getScanner(scan);
//                return new VariantHBaseScanIterator(resScan, genomeHelper, studyConfigurationManager, options);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
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


    @Override
    /**
     * Ensure that all the annotation fields exist are defined.
     */
    public void preUpdateStats(StudyConfiguration studyConfiguration) throws IOException {
        try {
            phoenixHelper.updateStatsFields(getJdbcConnection(), variantTable, studyConfiguration);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        return updateStats(variantStatsWrappers, studyName, queryOptions);
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

    @Override
    public QueryResult deleteStats(String studyName, String cohortName, QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    /**
     * Ensure that all the annotation fields exist are defined.
     */
    public void preUpdateAnnotations() throws IOException {
        try {
            phoenixHelper.updateAnnotationFields(getJdbcConnection(), variantTable);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public QueryResult deleteAnnotation(String annotationId, Query query, QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult addAnnotations(List<org.opencb.biodata.models.variant.avro.VariantAnnotation> variantAnnotations,
                                      QueryOptions queryOptions) {
        return updateAnnotations(variantAnnotations, queryOptions);
    }

    @Override
    public QueryResult updateAnnotations(List<org.opencb.biodata.models.variant.avro.VariantAnnotation> variantAnnotations,
                                         QueryOptions queryOptions) {

        long start = System.currentTimeMillis();

        VariantAnnotationToHBaseConverter converter = new VariantAnnotationToHBaseConverter(new GenomeHelper(configuration));
        List<Put> puts = converter.apply(variantAnnotations);

        try (Table table = getConnection().getTable(TableName.valueOf(variantTable))) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new QueryResult("Update annotations", (int) (System.currentTimeMillis() - start), 0, 0, "", "", Collections.emptyList());
    }

    public Connection getConnection() {
        return this.genomeHelper.getHBaseManager().getConnection();
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


    ////// Util methods:
    private Scan parseQuery(Query query, QueryOptions options) {

        Scan scan = new Scan();
        scan.addFamily(genomeHelper.getColumnFamily());
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        List<byte[]> columnPrefixes = new LinkedList<>();

        if (!StringUtils.isEmpty(query.getString(REGION.key()))) {
            Region region = Region.parseRegion(query.getString(REGION.key()));
            logger.debug("region = " + region);
            addRegionFilter(scan, region);
        } else {
            addDefaultRegionFilter(scan);
        }

        if (!StringUtils.isEmpty(query.getString(GENE.key()))) {
            addValueFilter(filters, GENES.bytes(), query.getAsStringList(GENE.key()));
        }
        if (!StringUtils.isEmpty(query.getString(ANNOT_BIOTYPE.key()))) {
            addValueFilter(filters, BIOTYPE.bytes(), query.getAsStringList(ANNOT_BIOTYPE.key()));
        }

        Set<String> includedFields = new HashSet<>(Arrays.asList("studies", "annotation"));
        if (!options.getAsStringList("include").isEmpty()) {
            includedFields = new HashSet<>(options.getAsStringList("include"));
        } else if (!options.getAsStringList("exclude").isEmpty()) {
            includedFields.removeAll(options.getAsStringList("exclude"));
        }

        if (includedFields.contains("studies")) {
            if (!StringUtils.isEmpty(query.getString(STUDIES.key()))) {
                //TODO: Handle negations(!), and(;), or(,) and studyName(string)
                List<Integer> studyIdList = query.getAsIntegerList(STUDIES.key());
                for (Integer studyId : studyIdList) {
                    columnPrefixes.add(Bytes.toBytes(studyId.toString() + genomeHelper.getSeparator()));
                }
            } else {
                for (int i = 0; i < 10; i++) {
                    columnPrefixes.add(Bytes.toBytes(Integer.toString(i)));
                }
            }
        }

        if (columnPrefixes.isEmpty()) {
            KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
            filters.addFilter(keyOnlyFilter);
        } else {
            MultipleColumnPrefixFilter columnPrefixFilter = new MultipleColumnPrefixFilter(columnPrefixes.toArray(new byte[columnPrefixes
                    .size()][]));
            filters.addFilter(columnPrefixFilter);
        }

        scan.setFilter(filters);
        scan.setMaxResultSize(options.getInt("limit"));

        logger.debug("StartRow = " + new String(scan.getStartRow()));
        logger.debug("StopRow = " + new String(scan.getStopRow()));
        logger.debug("MaxResultSize = " + scan.getMaxResultSize());
        logger.debug("Filters = " + scan.getFilter().toString());
        return scan;
    }

    private void addValueFilter(FilterList filters, byte[] column, List<String> values) {
        List<Filter> valueFilters = new ArrayList<>(values.size());
        for (String value : values) {
            SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(genomeHelper.getColumnFamily(),
                    column, CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
            valueFilter.setFilterIfMissing(true);
            valueFilters.add(valueFilter);
        }
        filters.addFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, valueFilters));
    }

    public void addArchiveRegionFilter(Scan scan, Region region, ArchiveHelper archiveHelper) {
        if (region == null) {
            addDefaultRegionFilter(scan);
        } else {
            scan.setStartRow(archiveHelper.generateBlockIdAsBytes(region.getChromosome(), region.getStart()));
            long endSlice = archiveHelper.getSliceId(region.getEnd()) + 1;
            // +1 because the stop row is exclusive
            scan.setStopRow(Bytes.toBytes(archiveHelper.generateBlockIdFromSlice(region.getChromosome(), endSlice)));
        }
    }

    public void addRegionFilter(Scan scan, Region region) {
        if (region == null) {
            addDefaultRegionFilter(scan);
        } else {
            scan.setStartRow(genomeHelper.generateVariantRowKey(region.getChromosome(), region.getStart()));
            scan.setStopRow(genomeHelper.generateVariantRowKey(region.getChromosome(), region.getEnd()));
        }
    }

    public Scan addDefaultRegionFilter(Scan scan) {
        return scan.setStopRow(Bytes.toBytes(String.valueOf(GenomeHelper.METADATA_PREFIX)));
    }


}
