package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mh719 on 15/12/2016.
 */
public class AnalysisTableAnnotateDriver extends AbstractAnalysisTableDriver {
    public static final String CONFIG_VARIANT_TABLE_ANNOTATE_PARALLEL = "opencga.variant.table.annotate.parallel";

    public AnalysisTableAnnotateDriver() {
        super();
    }

    public AnalysisTableAnnotateDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        int parallel = getConf().getInt(CONFIG_VARIANT_TABLE_ANNOTATE_PARALLEL, 5);
        getConf().setInt("mapreduce.job.running.map.limit", parallel);
        getConf().setLong("phoenix.upsert.batch.size", 200L);
    }

    @Override
    protected Class<AnalysisAnnotateMapper> getMapperClass() {
        return AnalysisAnnotateMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable, List<Integer> files) throws IOException {
        // QUERY design
        Scan scan = createVariantsTableScan();

        // set other scan attrs
        TableMapReduceUtil.setScannerCaching(job, 200);
        initMapReduceJob(job, getMapperClass(), variantTable, scan);

        String[] fieldNames = Arrays.stream(VariantColumn.values()).map(VariantColumn::toString).toArray(String[]::new);
        PhoenixMapReduceUtil.setOutput(job, SchemaUtil.getEscapedFullTableName(variantTable), fieldNames);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PhoenixVariantAnnotationWritable.class);
        job.setNumReduceTasks(0);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "Annotate";
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new AnalysisTableAnnotateDriver().privateMain(args));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
