package org.opencb.opencga.storage.hadoop.variant;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
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
import org.opencb.opencga.storage.hadoop.mr.GenomeHelper;
import org.opencb.opencga.storage.hadoop.mr.GenomeVariantHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by mh719 on 16/06/15.
 */
public class VariantHadoopDBAdaptor implements VariantDBAdaptor {
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");

    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantStorageManager.class);

    private final Connection con;
    private final Table table;
    private GenomeHelper genomeHelper;

    public VariantHadoopDBAdaptor(HadoopCredentials credentials, StorageEngineConfiguration configuration) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        for (Map.Entry<String, Object> entry : configuration.getVariant().getOptions().entrySet()) {
            conf.set(entry.getKey(), entry.getValue().toString());
        }

        // HBase configuration
        conf.set("hbase.master", credentials.getHost() + ":" + credentials.getHbasePort());
        conf.set("hbase.zookeeper.quorum", credentials.getHost());
//        conf.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));
//        conf.set("zookeeper.znode.parent", "/hbase");
        genomeHelper = new GenomeHelper(conf);

        con = ConnectionFactory.createConnection(conf);
        table = con.getTable(TableName.valueOf(credentials.getTable()));
    }

    public QueryResult<VcfSliceProtos.VcfMeta> getMeta(byte[] study) {
        Get get = new Get(genomeHelper.getMetaRowKey());
        get.addColumn(getColumnFamily(), study);
        try {
            Result result = table.get(get);
            byte[] value = result.getValue(getColumnFamily(), study);
            VcfSliceProtos.VcfMeta vcfMeta = VcfSliceProtos.VcfMeta.parseFrom(value);
            return new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(vcfMeta));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public GenomeVariantHelper getGenomeVariantHelper(byte[] studyIdBytes) {
        try {
            return new GenomeVariantHelper(genomeHelper, getMeta(studyIdBytes).first());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Deprecated
    public QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options) {
        long start = System.currentTimeMillis();
        QueryResult<Variant> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        List<Variant> results = new LinkedList<>();
        Scan scan = new Scan();

        scan.addFamily(getColumnFamily());
        scan.setStartRow(buildRowkey(region.getChromosome(),region.getStart()));
        scan.setStopRow(buildRowkey(region.getChromosome(),region.getEnd()));

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

    private byte[] buildRowkey(String chromosome, int position) {
        return buildRowkey(chromosome, Long.valueOf(position));
    }

    private byte[] buildRowkey(String chromosome, long position) {
        return genomeHelper.generateBlockIdAsBytes(chromosome, position);
    }

    private byte[] getColumnFamily(){
        return COLUMN_FAMILY;
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
        long start = System.currentTimeMillis();
        Region region = Region.parseRegion(query.getString(VariantQueryParams.REGION.key()));
        String studyId = query.getString(VariantQueryParams.STUDIES.key());
        byte[] studyIdBytes = studyId.getBytes();

        Scan scan = new Scan();
        scan.addFamily(getColumnFamily());
        scan.setStartRow(buildRowkey(region.getChromosome(),region.getStart()));
        scan.setStopRow(buildRowkey(region.getChromosome(),region.getEnd()));
        logger.debug(new String(scan.getStartRow()));
        logger.debug(new String(scan.getStopRow()));

        logger.debug("region = " + region);
        logger.debug("Created iterator");

        try {
            ResultScanner resScan = table.getScanner(scan);
            final Iterator<Result> iter = resScan.iterator();
            return new VariantHadoopDBIterator(iter, getColumnFamily(), studyIdBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    public StudyConfigurationManager getStudyConfigurationManager() {
        // TODO Auto-generated method stub
        return new StudyConfigurationManager(new ObjectMap()) {
            @Override
            protected QueryResult<StudyConfiguration> _getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
                return null;
            }

            @Override
            protected QueryResult<StudyConfiguration> _getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
                return null;
            }

            @Override
            protected QueryResult _updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
                return null;
            }

            @Override
            public List<String> getStudyNames(QueryOptions options) {
                return Collections.emptyList();
            }
        };
    }

    @Override
    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {
        // TODO Auto-generated method stub
        
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
