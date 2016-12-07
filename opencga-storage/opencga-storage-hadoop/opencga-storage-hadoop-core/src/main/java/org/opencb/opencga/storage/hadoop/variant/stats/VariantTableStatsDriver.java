package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;

import java.io.IOException;

/**
 * Created by mh719 on 21/11/2016.
 */
public class VariantTableStatsDriver extends AbstractAnalysisTableDriver {

    public VariantTableStatsDriver() { /* nothing */ }

    public VariantTableStatsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
    // nothing to do
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AnalysisStatsMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                inTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new VariantTableStatsDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
