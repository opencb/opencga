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
import org.opencb.hpg.bigdata.tools.utils.HBaseUtils;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHadoopArchiveDBIterator;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by mh719 on 16/06/15.
 */
public class VariantHadoopDBAdaptor implements VariantDBAdaptor {
    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantStorageManager.class);

    private final Connection con;
    // FIXME: Pooling or caching this object is not recommended.
    // Should create for each query and close it at the end.
    @Deprecated
    private final Table table;
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
        this.table = con.getTable(TableName.valueOf(variantTable));
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
    @Deprecated
    public QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options) {
        long start = System.currentTimeMillis();
        QueryResult<Variant> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        List<Variant> results = new LinkedList<>();
        Scan scan = new Scan();

        scan.addFamily(genomeHelper.getColumnFamily());
        scan.setStartRow(genomeHelper.generateBlockIdAsBytes(region.getChromosome(), Long.valueOf(region.getStart())));
        scan.setStopRow(genomeHelper.generateBlockIdAsBytes(region.getChromosome(), Long.valueOf(region.getEnd())));

        try {
            ResultScanner resScan = table.getScanner(scan);
            Iterator<Result> iter = resScan.iterator();
            while(iter.hasNext()){
                Result result = iter.next();
                byte[] rid = result.getRow();
                Variant var = buildVariantFromRowId(rid);
                results.add(var);
            }

            long end = System.currentTimeMillis();
            Long time = end-start;
            queryResult.setResult(results);
            queryResult.setNumResults(results.size());
            queryResult.setDbTime(time.intValue());

        } catch (IOException e) {
            String msg = String.format("Problems with query: %s", e);
            getLog().error(msg,e);
            queryResult.setErrorMsg(msg);
        }
        return queryResult;
    }

    public static Variant buildVariantFromRowId(byte[] rid) {
        String rowString = Bytes.toString(rid);

        String[] arr = StringUtils.split(rowString, HBaseUtils.ROWKEY_SEPARATOR);

        String chr = arr[0];
        int start = Long.valueOf(arr[1]).intValue();
        String ref = arr[2];
        String alt = arr.length > 3?arr[3]:null;

        int end = start+ref.length();
        end+=1; // 0 based adjustment TODO check if this is correct

        Variant var = new Variant(
                chr,start,end,ref,alt
        );
        return var;
    }

    @Deprecated
	@Override
    public List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
        List<QueryResult<Variant>> allResults;
        if (options == null) {
            options = new QueryOptions();
        }
        
        // If the users asks to sort the results, do it by chromosome and start
        if (options.getBoolean("sort", false)) {
            // TODO Add sort option
        }
        
        // If the user asks to merge the results, run only one query,
        // otherwise delegate in the method to query regions one by one
        if (options.getBoolean("merge", false)) {
            options.add(VariantQueryParams.REGION.key(), regionList);
            allResults = Collections.singletonList(getAllVariants(options));
        } else {
            allResults = new ArrayList<>(regionList.size());
            for (Region r : regionList) {
                QueryResult queryResult = getAllVariantsByRegion(r, options);
                queryResult.setId(r.toString());
                allResults.add(queryResult);
            }
        }
        return allResults;
    }


    @Override
    public boolean close() {
        try {
            table.close();
        } catch (IOException e) {
            getLog().error("Problems closing Table", e);
        }
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
        Region region = Region.parseRegion(query.getString(VariantQueryParams.REGION.key()));

        if (query.containsKey(VariantQueryParams.FILES.key())) {
            String study = query.getString(VariantQueryParams.STUDIES.key());
            StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, options).first();
            int studyId;
            if (StringUtils.isNumeric(study)) {
                studyId = Integer.parseInt(study);
            } else {
                studyId = studyConfiguration.getStudyId();
            }

            int fileId = query.getInt(VariantQueryParams.FILES.key());
            Scan scan = new Scan();
            scan.addFamily(genomeHelper.getColumnFamily());
            scan.setStartRow(genomeHelper.generateBlockIdAsBytes(region.getChromosome(), region.getStart()));
            scan.setStopRow(genomeHelper.generateBlockIdAsBytes(region.getChromosome(), region.getEnd()));
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
            scan.setStartRow(Bytes.toBytes(genomeHelper.generateRowPositionKey(region.getChromosome(), region.getStart())));
            scan.setStopRow(Bytes.toBytes(genomeHelper.generateRowPositionKey(region.getChromosome(), region.getEnd())));
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
    public QueryResult<Variant> getAllVariants(QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult<Variant> getVariantById(String id, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult groupBy(String field, QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VariantDBIterator iterator(QueryOptions options) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, int studyId, QueryOptions queryOptions) {
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


}
