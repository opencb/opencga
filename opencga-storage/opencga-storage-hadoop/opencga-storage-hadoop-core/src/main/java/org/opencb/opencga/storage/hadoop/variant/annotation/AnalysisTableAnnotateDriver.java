package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by mh719 on 15/12/2016.
 */
public class AnalysisTableAnnotateDriver extends AbstractAnalysisTableDriver {
    public static final String CONFIG_VARIANT_TABLE_ANNOTATE_PARALLEL = "opencga.variant.table.annotate.parallel";

    public AnalysisTableAnnotateDriver() { /* nothing */ }

    public AnalysisTableAnnotateDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        int parallel = getConf().getInt(CONFIG_VARIANT_TABLE_ANNOTATE_PARALLEL, 5);
        getConf().setInt("mapreduce.job.running.map.limit", parallel);
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AnalysisAnnotateMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);
        String[] fieldNames = Arrays.stream(VariantColumn.values()).map(v -> v.toString()).toArray(String[]::new);
        PhoenixMapReduceUtil.setOutput(job, SchemaUtil.getEscapedFullTableName(inTable), fieldNames);
        job.setOutputFormatClass(PhoenixOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PhoenixVariantAnnotationWritable.class);
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new AnalysisTableAnnotateDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
