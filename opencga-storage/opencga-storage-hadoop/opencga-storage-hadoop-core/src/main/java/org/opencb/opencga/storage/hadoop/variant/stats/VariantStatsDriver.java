package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsDriver;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Created on 15/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsDriver extends AbstractAnalysisTableDriver {
    public static final String STATS_INPUT = "stats.input";
    public static final String STATS_INPUT_DEFAULT = "hbase";
    private static final String STATS_OPERATION_NAME = "stats";

    private Collection<Integer> cohorts;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsDriver.class);

    public VariantStatsDriver() {
    }

    public VariantStatsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        cohorts = VariantStatsMapper.getCohorts(getConf());
    }

    @Override
    protected Class<VariantStatsMapper> getMapperClass() {
        return VariantStatsMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
//        if (getConf().get(STATS_INPUT, STATS_INPUT_DEFAULT).equalsIgnoreCase("phoenix")) {
//            try (HBaseStudyConfigurationDBAdaptor adaptor = new HBaseStudyConfigurationDBAdaptor(getAnalysisTable(), getConf(), null);
//                 StudyConfigurationManager scm = new StudyConfigurationManager(adaptor)) {
//                // Sql
//                Query query = buildQuery(getStudyId(), samples, getFiles());
//                QueryOptions options = buildQueryOptions();
//                String sql = new VariantSqlQueryParser(getHelper(), getAnalysisTable(), scm).parse(query,
//                        options).getSql();
//
//                logger.info("Query : " + query.toJson());
//                logger.info(sql);
//
//                // input
//                VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTableName, sql, FillGapsMapper.class);
//            }
//        } else {
            // scan
            // TODO: Improve filter!
            Scan scan = new Scan();
            scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator(getHelper().getMetaRowKeyString())));
            // input
            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTableName, scan, getMapperClass());
//        }
        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true, ".");

        // output
        VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        // only mapper
        VariantMapReduceUtil.setNoneReduce(job);

        VariantStatsMapper.setCohorts(job, cohorts);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return STATS_OPERATION_NAME;
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
