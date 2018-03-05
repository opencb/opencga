package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.tools.ant.types.Commandline;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsFromVariantTask.buildQuery;
import static org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsFromVariantTask.buildQueryOptions;

/**
 * Created on 30/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsDriver extends AbstractAnalysisTableDriver {

    public static final String FILL_GAPS_OPERATION_NAME = "fill_gaps";
    public static final String FILL_MISSING_OPERATION_NAME = "fill_missing";
    public static final String FILL_GAPS_INPUT = "fill-gaps.input";
    public static final String FILL_GAPS_INPUT_DEFAULT = "archive";
    private Collection<Integer> samples;
    private final Logger logger = LoggerFactory.getLogger(FillGapsDriver.class);

    public FillGapsDriver() {
    }

    public FillGapsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        samples = FillGapsFromArchiveMapper.getSamples(getConf());
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return FillGapsFromArchiveMapper.class;
    }

    @Override
    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        if (!FillGapsFromArchiveMapper.isFillGaps(getConf())) {
            try {
                logger.info("Prepare archive table for " + FILL_MISSING_OPERATION_NAME);
                String args = PrepareFillMissingDriver.buildCommandLineArgs(
                        getArchiveTable(), variantTable, getStudyId(), Collections.emptyList(), new ObjectMap());
                int exitValue = new PrepareFillMissingDriver().privateMain(Commandline.translateCommandline(args), getConf());
                if (exitValue != 0) {
                    throw new StorageEngineException("Error executing PrepareFillMissing");
                }
            } catch (Exception e) {
                throw new StorageEngineException("Error executing PrepareFillMissing", e);
            }
        }

    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
        String input = getConf().get(FILL_GAPS_INPUT, FILL_GAPS_INPUT_DEFAULT);
        if (input.equalsIgnoreCase("archive")) {
            // scan
            Scan scan;
            if (FillGapsFromArchiveMapper.isFillGaps(getConf())) {
                scan = FillGapsFromArchiveTask.buildScan(getFiles(), getConf().get(VariantQueryParam.REGION.key()), getConf());
            } else {
                scan = FillMissingFromArchiveTask.buildScan(getFiles(), getConf().get(VariantQueryParam.REGION.key()), getConf());
            }

            int caching = getConf().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 50);
            logger.info("Scan set Caching to " + caching);
            scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
            scan.setCacheBlocks(false);      // don't set to true for MR jobs
            logger.info("Scan archive table " + archiveTableName + " with scan " + scan.toString(50));

            // input
            VariantMapReduceUtil.initTableMapperJob(job, archiveTableName, variantTableName, scan, FillGapsFromArchiveMapper.class);
            job.getConfiguration().setInt(AbstractAnalysisTableDriver.TIMESTAMP, 5); // Not used, but must be defined
        } else if (input.equalsIgnoreCase("phoenix")) {
            // Sql
            Query query = buildQuery(getStudyId(), samples, getFiles());
            QueryOptions options = buildQueryOptions();
            String sql = new VariantSqlQueryParser(getHelper(), getAnalysisTable(), getStudyConfigurationManager())
                    .parse(query, options).getSql();

            logger.info("Query : " + query.toJson());
            logger.info(sql);

            // input
            VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTableName, sql, FillGapsMapper.class);
        } else {
            // scan
            Scan scan = new Scan();

            // input
            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTableName, scan, FillGapsMapper.class);
        }

        // output
        VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        // only mapper
        VariantMapReduceUtil.setNoneReduce(job);

        FillGapsFromArchiveMapper.setSamples(job, samples);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return FillGapsFromArchiveMapper.isFillGaps(getConf()) ? FILL_GAPS_OPERATION_NAME : FILL_MISSING_OPERATION_NAME;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new FillGapsDriver().privateMain(args));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
