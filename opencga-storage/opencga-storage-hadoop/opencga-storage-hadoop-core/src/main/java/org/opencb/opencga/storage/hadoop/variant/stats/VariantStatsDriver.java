package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.NavigableSet;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.STATS_DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter.endsWith;
import static org.opencb.opencga.storage.hadoop.variant.stats.HBaseVariantStatsCalculator.excludeFiles;

/**
 * Created on 15/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsDriver extends AbstractVariantsTableDriver {
    public static final String STATS_INPUT = "stats.input";
    public static final String STATS_INPUT_DEFAULT = "native";
    private static final String STATS_OPERATION_NAME = "stats";
    public static final String STATS_PARTIAL_RESULTS = "stats.partial-results";
    public static final boolean STATS_PARTIAL_RESULTS_DEFAULT = true;

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
    protected void preExecution() throws IOException, StorageEngineException {
        super.preExecution();
        StudyMetadata studyMetadata = getMetadataManager().getStudyMetadata(getStudyId());
        if (!HBaseToVariantConverter.getFixedFormat(studyMetadata.getAttributes()).contains("GT")) {
            throw new IllegalArgumentException("Study '" + studyMetadata.getName() + "' does not have Genotypes");
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
        ObjectMap options = new ObjectMap();
        getConf().iterator().forEachRemaining(entry -> options.put(entry.getKey(), entry.getValue()));

        boolean updateStats = options.getBoolean(VariantStorageEngine.Options.UPDATE_STATS.key(),
                VariantStorageEngine.Options.UPDATE_STATS.defaultValue());
        boolean overwrite = options.getBoolean(VariantStorageEngine.Options.OVERWRITE_STATS.key(),
                VariantStorageEngine.Options.OVERWRITE_STATS.defaultValue());
        boolean statsMultiAllelic = getConf().getBoolean(VariantStorageEngine.Options.STATS_MULTI_ALLELIC.key(),
                VariantStorageEngine.Options.STATS_MULTI_ALLELIC.defaultValue());
        String statsDefaultGenotype = getConf().get(VariantStorageEngine.Options.STATS_DEFAULT_GENOTYPE.key(),
                VariantStorageEngine.Options.STATS_DEFAULT_GENOTYPE.defaultValue());
        Query query = VariantStatisticsManager.buildInputQuery(getMetadataManager(), readStudyMetadata(),
                cohorts, overwrite, updateStats, options);
        QueryOptions queryOptions = VariantStatisticsManager.buildIncludeExclude();

        boolean excludeFiles = excludeFiles(statsMultiAllelic, statsDefaultGenotype);
        if (excludeFiles) {
            // Do not include files when not calculating multi-allelic frequencies.
            query.put(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.NONE);
        }
        // Allow partial results if files are not required
        boolean allowPartialResults = excludeFiles && getConf().getBoolean(STATS_PARTIAL_RESULTS, STATS_PARTIAL_RESULTS_DEFAULT);


        LOG.info("Query : " + query.toJson());

        if (getConf().get(STATS_INPUT, STATS_INPUT_DEFAULT).equalsIgnoreCase("native")) {
            // Some of the filters in query are not supported by VariantHBaseQueryParser
            Scan scan = new VariantHBaseQueryParser(getHelper(), getMetadataManager()).parseQuery(query, queryOptions);
            if (excludeFiles) {
                // Ensure we are not returning any file
                NavigableSet<byte[]> columns = scan.getFamilyMap().get(getHelper().getColumnFamily());
                columns.removeIf(column -> endsWith(column, VariantPhoenixHelper.FILE_SUFIX_BYTES));
            }
            scan.setCacheBlocks(false);
            int caching = getConf().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 50);
            LOG.info("Scan set Caching to " + caching);
            scan.setCaching(caching);
            LOG.info(scan.toString());

            // input + output
            if (allowPartialResults) {
                VariantMapReduceUtil.initPartialResultTableMapperJob(
                        job, variantTableName, variantTableName, scan, VariantStatsFromResultMapper.class);
            } else {
                VariantMapReduceUtil.initTableMapperJob(
                        job, variantTableName, variantTableName, scan, VariantStatsFromResultMapper.class);
            }
        } else if (getConf().get(STATS_INPUT, STATS_INPUT_DEFAULT).equalsIgnoreCase("phoenix")) {
            // Sql
            String sql = new VariantSqlQueryParser(getHelper(), getVariantsTable(), getMetadataManager())
                    .parse(query, queryOptions).getSql();

            LOG.info(sql);

            // input
            VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTableName, sql, VariantStatsMapper.class);
            // output
            VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        } else {
            // scan
            // TODO: Improve filter!
            // Some of the filters in query are not supported by VariantHBaseQueryParser
            Scan scan = new VariantHBaseQueryParser(getHelper(), getMetadataManager()).parseQuery(query, queryOptions);

            LOG.info(scan.toString());
            // input
            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTableName, scan, VariantStatsMapper.class);
            // output
            VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        }
        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true,
                options.getString(STATS_DEFAULT_GENOTYPE.key(), STATS_DEFAULT_GENOTYPE.defaultValue()));

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
