package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHadoopArchiveDBIterator;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseResultSetIterator;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseScanIterator;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.VariantAnnotationToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.BIOTYPE;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.GENES;

/**
 * Created by mh719 on 16/06/15.
 */
public class VariantHadoopDBAdaptor implements VariantDBAdaptor {
    protected static Logger logger = LoggerFactory.getLogger(VariantHadoopDBAdaptor.class);

    private final Connection hbaseCon;
    private final String variantTable;
    private final VariantPhoenixHelper phoenixHelper;
    private StudyConfigurationManager studyConfigurationManager;
    private final Configuration configuration;
    private GenomeHelper genomeHelper;
    private final java.sql.Connection phoenixCon;
    private final VariantSqlQueryParser queryParser;
//    private final PhoenixConnection phoenixCon;

    public VariantHadoopDBAdaptor(HadoopCredentials credentials, StorageEngineConfiguration configuration,
                                  Configuration conf) throws IOException {
        conf = getHbaseConfiguration(conf, credentials);

        this.configuration = conf;
        genomeHelper = new GenomeHelper(this.configuration);

        hbaseCon = ConnectionFactory.createConnection(conf);
        variantTable = credentials.getTable();
        studyConfigurationManager = new HBaseStudyConfigurationManager(credentials, conf, configuration.getVariant().getOptions());

        queryParser = new VariantSqlQueryParser(genomeHelper, variantTable, new VariantDBAdaptorUtils(this));

        phoenixHelper = new VariantPhoenixHelper(genomeHelper);
        try {
            phoenixCon = phoenixHelper.newJdbcConnection(conf);
        } catch (SQLException | ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public java.sql.Connection getJdbcConnection() {
        return phoenixCon;
    }

    public GenomeHelper getGenomeHelper() {
        return genomeHelper;
    }

    static Configuration getHbaseConfiguration(Configuration configuration, HadoopCredentials credentials) {
        configuration = HBaseConfiguration.create(configuration);

        // HBase configuration
        configuration.set(HConstants.ZOOKEEPER_QUORUM, credentials.getHost());
//        configuration.set("hbase.master", credentials.getHost() + ":" + credentials.getHbasePort());
//        configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));
//        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
        return configuration;
    }

    public ArchiveHelper getArchiveHelper(int studyId, int fileId) throws IOException {
        VcfMeta vcfMeta = getVcfMeta(ArchiveHelper.getTableName(studyId), fileId, null).first();
        if (vcfMeta == null) {
            throw new IOException("File '" + fileId + "' not found in study '" + studyId + "'");
        }
        return new ArchiveHelper(genomeHelper, vcfMeta);

    }

    public QueryResult<VcfMeta> getVcfMeta(String tableName, int fileId, ObjectMap options) throws IOException {
        try (ArchiveFileMetadataManager manager = getArchiveFileMetadataManager(tableName, options)) {
            return manager.getVcfMeta(fileId, options);
        }
    }

    /**
     * @param tableName Use {@link ArchiveHelper#getTableName(int)} to get the table
     * @param options   Extra options
     * @return A valid ArchiveFileMetadataManager object
     * @throws IOException If any IO problem occurs
     */
    public ArchiveFileMetadataManager getArchiveFileMetadataManager(String tableName, ObjectMap options)
            throws IOException {
        return new ArchiveFileMetadataManager(tableName, configuration, options);
    }


    @Override
    public StudyConfigurationManager getStudyConfigurationManager() {
        return studyConfigurationManager;
    }

    @Override
    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;
    }

    @Override
    public boolean close() {
        boolean closeCorrect = true;
        try {
            if (!hbaseCon.isClosed()) {
                hbaseCon.close();
            }
        } catch (IOException e) {
            getLog().error("Problems closing connection", e);
            closeCorrect = false;
        }

        try {
            if (phoenixCon != null && !phoenixCon.isClosed()) {
                phoenixCon.close();
            }
        } catch (SQLException e) {
            getLog().error("Problems closing connection", e);
            closeCorrect = false;
        }

        return closeCorrect;
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
        long numTotalResults = -1;
        if (options != null && !options.getBoolean("skipCount")) {
            numTotalResults = count(query).first();
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
    public QueryResult<Long> count(Query query) {

        long startTime = System.currentTimeMillis();
        String sql = queryParser.parse(query, new QueryOptions(VariantSqlQueryParser.COUNT, true));
        try {
            Statement statement = phoenixCon.createStatement();
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

        if (options.getBoolean("archive", false)) {
            String study = query.getString(STUDIES.key());
            StudyConfiguration studyConfiguration;
            int studyId;
            if (StringUtils.isNumeric(study)) {
                studyId = Integer.parseInt(study);
                studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, options).first();
            } else {
                studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, options).first();
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
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            Scan scan = new Scan();
            scan.addColumn(archiveHelper.getColumnFamily(), Bytes.toBytes(ArchiveHelper.getColumnName(fileId)));
            addArchiveRegionFilter(scan, region, archiveHelper);
            scan.setMaxResultSize(options.getInt("limit"));
            String tableName = ArchiveHelper.getTableName(studyId);

            logger.debug("Creating {} iterator", VariantHadoopArchiveDBIterator.class);
            logger.debug("Table name = " + tableName);
            logger.debug("StartRow = " + new String(scan.getStartRow()));
            logger.debug("StopRow = " + new String(scan.getStopRow()));
            logger.debug("MaxResultSize = " + scan.getMaxResultSize());
            logger.debug("region = " + region);
            logger.debug("Column name = " + fileId);
            logger.debug("Chunk size = " + archiveHelper.getChunkSize());

            try {
                Table table = hbaseCon.getTable(TableName.valueOf(tableName));
                ResultScanner resScan = table.getScanner(scan);
                return new VariantHadoopArchiveDBIterator(resScan, archiveHelper, options).setRegion(region);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            logger.debug("Creating {} iterator", VariantHBaseScanIterator.class);
            logger.debug("Table name = " + variantTable);
            String sql = queryParser.parse(query, options);
            logger.info(sql);

//            try (Statement statement = phoenixCon.createStatement()) {
//                ResultSet resultSet = statement.executeQuery(sql);
//                return new VariantHBaseResultIterator(resultSet, genomeHelper, studyConfigurationManager, options);
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
            try {
                Statement statement = phoenixCon.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                return new VariantHBaseResultSetIterator(resultSet, genomeHelper, studyConfigurationManager, options);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }


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
    public Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration,
                                   QueryOptions options) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
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
            phoenixHelper.updateAnnotationFields(phoenixCon, variantTable);
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

        try (Table table = hbaseCon.getTable(TableName.valueOf(variantTable))) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new QueryResult("Update annotations", (int) (System.currentTimeMillis() - start), 0, 0, "", "", Collections.emptyList());
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
            long endSlice = archiveHelper.getSlicePosition(region.getEnd()) + 1;
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
