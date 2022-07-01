package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.STATS_DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter.endsWith;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.stats.HBaseVariantStatsCalculator.excludeFiles;

/**
 * Created on 15/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsDriver extends AbstractVariantsTableDriver {
    private static final String STATS_OPERATION_NAME = "stats";
    public static final String STATS_PARTIAL_RESULTS = "stats.partial-results";
    public static final String OUTPUT = "output";
    public static final String COHORTS = "cohorts";
    public static final boolean STATS_PARTIAL_RESULTS_DEFAULT = true;

    private Collection<Integer> cohorts;
    private static Logger logger = LoggerFactory.getLogger(VariantStatsDriver.class);

    private Aggregation aggregation;
    private boolean overwrite;
    private boolean statsMultiAllelic;
    private String statsDefaultGenotype;
    private boolean excludeFiles;
    private MapReduceOutputFile output;

    public VariantStatsDriver() {
    }

    public VariantStatsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("--" + VariantStorageOptions.STUDY.key(), "<study>*");
        params.put("--" + COHORTS, "<cohorts>*");
        params.put("--" + OUTPUT, "<output>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        String[] split = getParam(COHORTS, "").split(",");
        cohorts = new ArrayList<>(split.length);
        List<String> cohortNames = new ArrayList<>(split.length);
        for (String cohort : split) {
            CohortMetadata cohortMetadata = getMetadataManager().getCohortMetadata(getStudyId(), cohort);
            cohorts.add(cohortMetadata.getId());
            cohortNames.add(cohortMetadata.getName());
        }

        aggregation = VariantStatsMapper.getAggregation(getConf());
        overwrite = getConf().getBoolean(VariantStorageOptions.STATS_OVERWRITE.key(),
                VariantStorageOptions.STATS_OVERWRITE.defaultValue());
        statsMultiAllelic = getConf().getBoolean(VariantStorageOptions.STATS_MULTI_ALLELIC.key(),
                VariantStorageOptions.STATS_MULTI_ALLELIC.defaultValue());
        statsDefaultGenotype = getConf().get(VariantStorageOptions.STATS_DEFAULT_GENOTYPE.key(),
                VariantStorageOptions.STATS_DEFAULT_GENOTYPE.defaultValue());
        excludeFiles = excludeFiles(statsMultiAllelic, statsDefaultGenotype, aggregation);

        logger.info(" * Aggregation: " + aggregation);
        logger.info(" * " + VariantStorageOptions.STATS_MULTI_ALLELIC.key() + ": " + statsMultiAllelic);
        logger.info(" * " + VariantStorageOptions.STATS_DEFAULT_GENOTYPE.key() + ": " + statsDefaultGenotype);


        output = new MapReduceOutputFile(() -> "variant_stats."
                + (cohorts.size() < 10 ? "." + String.join("_", cohortNames) : "")
                + TimeUtils.getTime() + ".json", "opencga_sample_variant_stats");
    }

    @Override
    protected void preExecution() throws IOException, StorageEngineException {
        super.preExecution();
        StudyMetadata studyMetadata = getMetadataManager().getStudyMetadata(getStudyId());
        if (!HBaseToVariantConverter.getFixedFormat(studyMetadata).contains("GT")) {
            throw new IllegalArgumentException("Study '" + studyMetadata.getName() + "' does not have Genotypes");
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
        ObjectMap options = new ObjectMap();
        getConf().iterator().forEachRemaining(entry -> options.put(entry.getKey(), entry.getValue()));

        Query query = VariantStatisticsManager.buildInputQuery(getMetadataManager(), readStudyMetadata(),
                cohorts, options, aggregation);
        QueryOptions queryOptions = VariantStatisticsManager.buildIncludeExclude();

        if (excludeFiles) {
            // Do not include files when not calculating multi-allelic frequencies.
            query.put(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.NONE);
        }

        if (output.getOutdir() != null) {
            // Do not index stats.
            // Allow any input query.
            // Write stats to file.
            // Use "VariantRow" as input
            Query queryFromParams = getQueryFromConfig(getConf());
            queryFromParams.putAll(query);
            queryFromParams.remove(VariantQueryParam.COHORT.key());
            logger.info("Query : " + query.toJson());

            queryOptions = new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES_STATS);

            // input
            VariantMapReduceUtil.initVariantRowMapperJob(job, VariantStatsFromVariantRowTsvMapper.class,
                    variantTableName, getMetadataManager(), queryFromParams, queryOptions, true);
            VariantMapReduceUtil.setNoneTimestamp(job);

            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setCompressOutput(job, false);
            TextOutputFormat.setOutputPath(job, output.getOutdir());

        } else if (AggregationUtils.isAggregated(aggregation)) {
            // For aggregated variants use plain VariantStatsMapper
            // Use "Variant" as input
            // Write results in HBase
            logger.info("Query : " + query.toJson());
            // input
            VariantMapReduceUtil.initVariantMapperJob(
                    job, VariantStatsMapper.class, variantTableName, getMetadataManager(), query, queryOptions, true);
            VariantMapReduceUtil.setNoneTimestamp(job);

            // output
            VariantMapReduceUtil.setOutputHBaseTable(job, variantTableName);
        } else {
            // For general stats, use native implementation VariantStatsFromResultMapper
            // Allow partial results by default
            // Write results in HBase

            logger.info("Query : " + query.toJson());
            // Some of the filters in query are not supported by VariantHBaseQueryParser
            Scan scan = new VariantHBaseQueryParser(getMetadataManager()).parseQuery(query, queryOptions);
            if (excludeFiles) {
                // Ensure we are not returning any file
                NavigableSet<byte[]> columns = scan.getFamilyMap().get(GenomeHelper.COLUMN_FAMILY_BYTES);
                columns.removeIf(column -> endsWith(column, VariantPhoenixSchema.FILE_SUFIX_BYTES));
            }
            // See #1600
            // Add TYPE column to force scan ALL rows to avoid unlikely but possible timeouts fetching new variants
            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.TYPE.bytes());
            // Remove STUDY filter
            scan.setFilter(null);

            VariantMapReduceUtil.configureMapReduceScan(scan, getConf());
            logger.info(scan.toString());

            // Allow partial results if files are not required
            boolean allowPartialResults = excludeFiles && getConf().getBoolean(STATS_PARTIAL_RESULTS, STATS_PARTIAL_RESULTS_DEFAULT);
            logger.info(" * Allow partial results : " + allowPartialResults);

            // input + output
            if (allowPartialResults) {
                VariantMapReduceUtil.initPartialResultTableMapperJob(
                        job, variantTableName, variantTableName, scan, VariantStatsFromResultMapper.class);
            } else {
                VariantMapReduceUtil.initTableMapperJob(
                        job, variantTableName, variantTableName, scan, VariantStatsFromResultMapper.class);
            }
        }
        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true,
                options.getString(STATS_DEFAULT_GENOTYPE.key(), STATS_DEFAULT_GENOTYPE.defaultValue()));

        // only mapper
        VariantMapReduceUtil.setNoneReduce(job);

        VariantStatsMapper.setCohorts(job, cohorts);

        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        output.postExecute(succeed);
    }

    @Override
    protected String getJobOperationName() {
        return STATS_OPERATION_NAME;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends Tool>) MethodHandles.lookup().lookupClass());
    }
}
