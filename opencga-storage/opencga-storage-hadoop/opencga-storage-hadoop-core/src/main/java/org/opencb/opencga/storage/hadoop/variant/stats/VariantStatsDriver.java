package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
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
public class VariantStatsDriver extends AbstractVariantsTableDriver {
    public static final String STATS_INPUT = "stats.input";
    public static final String STATS_INPUT_DEFAULT = "native";
    private static final String STATS_OPERATION_NAME = "stats";

    private Collection<Integer> cohorts;
    private static final Logger LOG = LoggerFactory.getLogger(VariantStatsDriver.class);

    public VariantStatsDriver() {
    }

    public VariantStatsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        cohorts = VariantStatsMapper.getCohorts(getConf());
    }

    @Override
    protected Class<VariantStatsFromResultMapper> getMapperClass() {
        return VariantStatsFromResultMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
        ObjectMap options = new ObjectMap();
        getConf().iterator().forEachRemaining(entry -> options.put(entry.getKey(), entry.getValue()));

        boolean updateStats = options.getBoolean(VariantStorageEngine.Options.UPDATE_STATS.key(),
                VariantStorageEngine.Options.UPDATE_STATS.defaultValue());
        boolean overwrite = options.getBoolean(VariantStorageEngine.Options.OVERWRITE_STATS.key(),
                VariantStorageEngine.Options.OVERWRITE_STATS.defaultValue());
        Query query = VariantStatisticsManager.buildInputQuery(readStudyConfiguration(), cohorts, overwrite, updateStats, options);
        QueryOptions queryOptions = VariantStatisticsManager.buildIncludeExclude();
        LOG.info("Query : " + query.toJson());

        if (getConf().get(STATS_INPUT, STATS_INPUT_DEFAULT).equalsIgnoreCase("native")) {
            // Some of the filters in query are not supported by VariantHBaseQueryParser
            Scan scan = new VariantHBaseQueryParser(getHelper(), getStudyConfigurationManager()).parseQuery(query, queryOptions);

            LOG.info(scan.toString());

            // input
            VariantMapReduceUtil.initTableMapperJob(job, variantTableName, variantTableName, scan, VariantStatsFromResultMapper.class);
        } else if (getConf().get(STATS_INPUT, STATS_INPUT_DEFAULT).equalsIgnoreCase("phoenix")) {
            // Sql
            String sql = new VariantSqlQueryParser(getHelper(), getVariantsTable(), getStudyConfigurationManager())
                    .parse(query, queryOptions).getSql();

            LOG.info(sql);

            // input
            VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTableName, sql, VariantStatsMapper.class);
        } else {
            // scan
            // TODO: Improve filter!
            // Some of the filters in query are not supported by VariantHBaseQueryParser
            Scan scan = new VariantHBaseQueryParser(getHelper(), getStudyConfigurationManager()).parseQuery(query, queryOptions);

            LOG.info(scan.toString());
            // input
            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTableName, scan, VariantStatsMapper.class);
        }
        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true,
                VariantStatisticsManager.UNKNOWN_GENOTYPE);

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
            System.exit(new VariantStatsDriver().privateMain(args));
        } catch (Exception e) {
            LOG.error("Error executing " + VariantStatsDriver.class, e);
            System.exit(1);
        }
    }
}
