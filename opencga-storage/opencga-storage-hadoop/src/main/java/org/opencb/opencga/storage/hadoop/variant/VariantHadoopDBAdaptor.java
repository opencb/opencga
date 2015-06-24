package org.opencb.opencga.storage.hadoop.variant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.hpg.bigdata.core.utils.HBaseUtils;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mh719 on 16/06/15.
 */
public class VariantHadoopDBAdaptor implements VariantDBAdaptor {
    public final static byte[] COLUMN_FAMILY = Bytes.toBytes("d");

    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantStorageManager.class);

    private final Connection con;
    private final Table table;

    public VariantHadoopDBAdaptor(HadoopCredentials credentials) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        // HBase configuration
        conf.set("hbase.master", credentials.getHost() + ":" + credentials.getHbasePort());
        conf.set("hbase.zookeeper.quorum", credentials.getHost());
//        conf.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));
//        conf.set("zookeeper.znode.parent", "/hbase");

        con = ConnectionFactory.createConnection(conf);
        table = con.getTable(TableName.valueOf(credentials.getTable()));
    }

    @Override
    public void setDataWriter(DataWriter dataWriter) {

    }

    @Override
    public void setConstantSamples(String sourceEntry) {

    }

    @Override
    public QueryResult<Variant> getAllVariants(QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult<Variant> getVariantById(String id, QueryOptions options) {
        return null;
    }

    @Override
    public List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
        return null;
    }

    @Override
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

    @Override
    public List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
        List<QueryResult<Variant>> allResults;
        if (options == null) {
            options = new QueryOptions();
        }
        
        // If the users asks to sort the results, do it by chromosome and start
        if (options.getBoolean(SORT, false)) {
            // TODO Add sort option
        }
        
        // If the user asks to merge the results, run only one query,
        // otherwise delegate in the method to query regions one by one
        if (options.getBoolean(MERGE, false)) {
            options.add(REGION, regionList);
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
    public QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult groupBy(String field, QueryOptions options) {
        return null;
    }

    @Override
    public VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return null;
    }

    @Override
    public VariantDBIterator iterator() {
        return null;
    }

    @Override
    public VariantDBIterator iterator(QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {
        return null;
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration, QueryOptions queryOptions) {
        return null;
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

    @Override
    public QueryResult getAllVariantsByRegionAndStudies(Region region, List<String> studyIds, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getAllVariantsByGene(String geneName, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getMostAffectedGenes(int numGenes, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getLeastAffectedGenes(int numGenes, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getTopConsequenceTypes(int numConsequenceTypes, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getBottomConsequenceTypes(int numConsequenceTypes, QueryOptions options) {
        return null;
    }

    private byte[] buildRowkey(String chromosome, String position) {
        return buildRowkey(chromosome,Long.valueOf(position));
    }
    private byte[] buildRowkey(String chromosome, int position) {
        return buildRowkey(chromosome, Long.valueOf(position));
    }

    private byte[] buildRowkey(String chromosome, long position) {
        String key = HBaseUtils.buildStoragePosition(chromosome, position);
        byte[] bKey = Bytes.toBytes(key);
        return bKey;
    }

    private byte[] getColumnFamily(){
        return COLUMN_FAMILY;
    }

    public static Logger getLog() {
        return logger;
    }
}
