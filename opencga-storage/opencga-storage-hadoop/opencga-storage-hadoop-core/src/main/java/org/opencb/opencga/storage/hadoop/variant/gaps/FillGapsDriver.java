package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsTask.buildQuery;
import static org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsTask.buildQueryOptions;

/**
 * Created on 30/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsDriver extends AbstractAnalysisTableDriver {

    public static final String FILL_GAPS_OPERATION_NAME = "fill_gaps";
    public static final String FILL_GAPS_INPUT = "fill-gaps.input";
    public static final String FILL_GAPS_INPUT_DEFAULT = "phoenix";
    private Collection<Integer> samples;
    private final Logger logger = LoggerFactory.getLogger(FillGapsDriver.class);

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
        if (getConf().get(FILL_GAPS_INPUT, FILL_GAPS_INPUT_DEFAULT).equalsIgnoreCase("phoenix")) {
            try (HBaseStudyConfigurationDBAdaptor adaptor = new HBaseStudyConfigurationDBAdaptor(getAnalysisTable(), getConf(), null);
                 StudyConfigurationManager scm = new StudyConfigurationManager(adaptor)) {
                // Sql
                Query query = buildQuery(getStudyId(), samples, getFiles());
                QueryOptions options = buildQueryOptions();
                String sql = new VariantSqlQueryParser(getHelper(), getAnalysisTable(), scm).parse(query,
                        options).getSql();

                logger.info("Query : " + query.toJson());
                logger.info(sql);

                // input
                VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTableName, sql, FillGapsMapper.class);
            }
        } else {
            // scan
            Scan scan = new Scan();
            scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator(getHelper().getMetaRowKeyString())));
            // input
            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTableName, scan, FillGapsMapper.class);
        }

        // output
        VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        // only mapper
        VariantMapReduceUtil.setNoneReduce(job);

        FillGapsMapper.setSamples(job, samples);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return FILL_GAPS_OPERATION_NAME;
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
