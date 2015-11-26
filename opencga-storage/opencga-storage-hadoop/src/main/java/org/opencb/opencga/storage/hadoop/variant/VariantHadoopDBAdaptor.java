package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHadoopArchiveDBIterator;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Consumer;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * Created by mh719 on 16/06/15.
 */
public class VariantHadoopDBAdaptor implements VariantDBAdaptor {
    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantStorageManager.class);

    private final Connection con;
    private final String variantTable;
    private StudyConfigurationManager studyConfigurationManager;
    private final Configuration configuration;
    private GenomeHelper genomeHelper;

    public VariantHadoopDBAdaptor(HadoopCredentials credentials, StorageEngineConfiguration configuration,
                                  Configuration conf) throws IOException {
        conf = getHbaseConfiguration(conf, credentials);

        this.configuration = conf;
        genomeHelper = new GenomeHelper(this.configuration);

        con = ConnectionFactory.createConnection(conf);
        variantTable = credentials.getTable();
        studyConfigurationManager = new HBaseStudyConfigurationManager(credentials, conf, configuration.getVariant().getOptions());
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
     *
     * @param tableName Use {@link ArchiveHelper#getTableName(int)} to get the table
     * @param options   Extra options
     * @throws IOException
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
        try {
            if(!con.isClosed()){
                con.close();
            }
            return true;
        } catch (IOException e) {
            getLog().error("Problems closing connection",e);
        }
        return false;
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
        return null;
    }

    @Override
    public QueryResult delete(Query query, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult deleteSamples(String studyName, List<String> sampleNames, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult deleteFile(String studyName, String fileName, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult deleteStudy(String studyName, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult<Variant> get(Query query, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<QueryResult<Variant>> get(List<Query> queries, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult<Long> count(Query query) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VariantDBIterator iterator() {
        return iterator(new Query(), new QueryOptions());
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        Region region = Region.parseRegion(query.getString(REGION.key()));

        if (query.containsKey(FILES.key())) {
            String study = query.getString(STUDIES.key());
            StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, options).first();
            int studyId;
            if (StringUtils.isNumeric(study)) {
                studyId = Integer.parseInt(study);
            } else {
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

            Scan scan = new Scan();
            scan.addColumn(genomeHelper.getColumnFamily(), Bytes.toBytes(ArchiveHelper.getColumnName(fileId)));
            addArchiveRegionFilter(scan, region);
            scan.setMaxResultSize(options.getInt("limit"));
            String tableName = ArchiveHelper.getTableName(studyId);

            logger.debug("Creating {} iterator", VariantHadoopArchiveDBIterator.class);
            logger.debug("Table name = " + tableName);
            logger.debug("StartRow = " + new String(scan.getStartRow()));
            logger.debug("StopRow = " + new String(scan.getStopRow()));
            logger.debug("MaxResultSize = " + scan.getMaxResultSize());
            logger.debug("region = " + region);
            logger.debug("Column name = " + fileId);

            try {
                ArchiveHelper archiveHelper = getArchiveHelper(studyId, fileId);
                Table table = con.getTable(TableName.valueOf(tableName));
                ResultScanner resScan = table.getScanner(scan);
                return new VariantHadoopArchiveDBIterator(resScan, archiveHelper, options);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {

            Scan scan = new Scan();
            scan.addFamily(genomeHelper.getColumnFamily());
            addRegionFilter(scan, region);
            scan.setMaxResultSize(options.getInt("limit"));

            logger.debug("Creating {} iterator", VariantHBaseIterator.class);
            logger.debug("Table name = " + variantTable);
            logger.debug("StartRow = " + new String(scan.getStartRow()));
            logger.debug("StopRow = " + new String(scan.getStopRow()));
            logger.debug("MaxResultSize = " + scan.getMaxResultSize());
            logger.debug("region = " + region);

            try {
                Table table = con.getTable(TableName.valueOf(variantTable));
                ResultScanner resScan = table.getScanner(scan);
                return  new VariantHBaseIterator(resScan, genomeHelper, studyConfigurationManager, options);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void forEach(Consumer<? super Variant> action) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void forEach(Query query, Consumer<? super Variant> action, QueryOptions options) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public QueryResult getFrequency(Query query, Region region, int regionIntervalSize) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration,
            QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult deleteStats(String studyName, String cohortName, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult deleteAnnotation(String annotationId, Query query, QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult addAnnotations(List<org.opencb.biodata.models.variant.avro.VariantAnnotation> variantAnnotations,
            QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult updateAnnotations(List<org.opencb.biodata.models.variant.avro.VariantAnnotation> variantAnnotations,
            QueryOptions queryOptions) {
        // TODO Auto-generated method stub
        return null;
    }


    ////// Util methods:

    public void addArchiveRegionFilter(Scan scan, Region region) {
        scan.setStartRow(genomeHelper.generateBlockIdAsBytes(region.getChromosome(), region.getStart()));
        scan.setStopRow(genomeHelper.generateBlockIdAsBytes(region.getChromosome(), region.getEnd()));
    }

    public void addRegionFilter(Scan scan, Region region) {
        scan.setStartRow(Bytes.toBytes(genomeHelper.generateRowPositionKey(region.getChromosome(), region.getStart())));
        scan.setStopRow(Bytes.toBytes(genomeHelper.generateRowPositionKey(region.getChromosome(), region.getEnd())));
    }


}
