package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.core.common.IOUtils.humanReadableByteCount;
import static org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsFromVariantTask.buildQuery;
import static org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsFromVariantTask.buildQueryOptions;

/**
 * Created on 30/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsDriver extends AbstractVariantsTableDriver {

    public static final String FILL_GAPS_OPERATION_NAME = "fill_gaps";
    public static final String FILL_MISSING_OPERATION_NAME = "fill_missing";
    public static final String FILL_GAPS_INPUT = "fill-gaps.input";
    public static final String FILL_GAPS_INPUT_DEFAULT = "archive";
    public static final String FILL_MISSING_INTERMEDIATE_FILE = "fill_missing.intermediate.file";
    private Collection<Integer> samples;
    private final Logger logger = LoggerFactory.getLogger(FillGapsDriver.class);
    private String input;
    private String outputPath;

    public FillGapsDriver() {
    }

    public FillGapsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        samples = FillGapsFromArchiveMapper.getSamples(getConf());
        input = getConf().get(FILL_GAPS_INPUT, FILL_GAPS_INPUT_DEFAULT);
        outputPath = getConf().get(FILL_MISSING_INTERMEDIATE_FILE);
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return FillGapsFromArchiveMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
        if (input.equalsIgnoreCase("archive")) {
            // scan
            List<Scan> scans;
            boolean fillGaps = FillGapsFromArchiveMapper.isFillGaps(getConf());
            String regionStr = getConf().get(VariantQueryParam.REGION.key());
            if (fillGaps) {
                scans = Collections.singletonList(FillGapsFromArchiveTask.buildScan(getFiles(), regionStr, getConf()));
            } else {
                scans = FillMissingFromArchiveTask.buildScan(getFiles(), regionStr, getConf());
            }

            int caching = getConf().getInt(HadoopVariantStorageOptions.MR_HBASE_SCAN_CACHING.key(), 50);
            logger.info("Scan set Caching to " + caching);
            for (int i = 0; i < scans.size(); i++) {
                Scan scan = scans.get(i);
                scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
                scan.setCacheBlocks(false);      // don't set to true for MR jobs
                logger.info("[" + i + "] Scan archive table " + archiveTableName + " with scan " + scan.toString(50));
            }

            if (fillGaps) {
                VariantMapReduceUtil.initTableMapperMultiOutputJob(job, archiveTableName, scans, FillGapsFromArchiveMapper.class);
            } else {
                // input
                VariantMapReduceUtil.initTableMapperJob(job, archiveTableName, scans, FillMissingFromArchiveMapper.class);

                // output
                job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
                job.setMapOutputKeyClass(BytesWritable.class);
                job.setMapOutputValueClass(BytesWritable.class);
                logger.info("Using intermediate file : " + outputPath);
                FileOutputFormat.setOutputPath(job, new Path(outputPath));
                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job, DeflateCodec.class);
                job.getConfiguration().set(FileOutputFormat.COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.name());
            }
        } else if (input.equalsIgnoreCase("phoenix")) {
            // Sql
            Query query = buildQuery(getStudyId(), samples, getFiles());
            QueryOptions options = buildQueryOptions();
            String sql = new VariantSqlQueryParser(getHelper(), getVariantsTable(), getMetadataManager())
                    .parse(query, options);

            logger.info("Query : " + query.toJson());
            logger.info(sql);

            // input
            VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTableName, sql, FillGapsMapper.class);
            // output
            VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        } else {
            // scan
            Scan scan = new Scan();

            // input
            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTableName, scan, FillGapsMapper.class);
            // output
            VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        }

        // only mapper
        VariantMapReduceUtil.setNoneReduce(job);

        FillGapsFromArchiveMapper.setSamples(job, samples);

        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            if (input.equalsIgnoreCase("archive") && StringUtils.isNotEmpty(outputPath)) {
                FileSystem fs = FileSystem.get(getConf());
                ContentSummary contentSummary = fs.getContentSummary(new Path(outputPath));
                logger.info("Generated file " + outputPath);
                logger.info(" - Size (HDFS)         : " + humanReadableByteCount(contentSummary.getLength(), false));
                logger.info(" - SpaceConsumed (raw) : " + humanReadableByteCount(contentSummary.getSpaceConsumed(), false));
            }
        }
    }

    @Override
    protected String getJobOperationName() {
        String regionStr = getConf().get(VariantQueryParam.REGION.key());
        return (FillGapsFromArchiveMapper.isFillGaps(getConf()) ? FILL_GAPS_OPERATION_NAME : FILL_MISSING_OPERATION_NAME)
                + (StringUtils.isNotEmpty(regionStr) ? "_" + regionStr : "");
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
