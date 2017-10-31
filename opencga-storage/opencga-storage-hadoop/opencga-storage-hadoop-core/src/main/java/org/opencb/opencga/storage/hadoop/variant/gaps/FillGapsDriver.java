package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;

import java.io.IOException;
import java.util.Collection;

/**
 * Created on 30/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsDriver extends AbstractAnalysisTableDriver {

    private Collection<Integer> samples;

    public FillGapsDriver() {
    }

    public FillGapsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        samples = FillGapsMapper.getSamples(getConf());
    }

    @Override
    protected Class<?> getMapperClass() {
        return FillGapsMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
        // scan
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator(getHelper().getMetaRowKeyString())));
        // input
        VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTableName, scan, FillGapsMapper.class);

        // output
        VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        // only mapper
        VariantMapReduceUtil.setNoneReduce(job);

        FillGapsMapper.setSamples(job, samples);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "fill-gaps";
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
