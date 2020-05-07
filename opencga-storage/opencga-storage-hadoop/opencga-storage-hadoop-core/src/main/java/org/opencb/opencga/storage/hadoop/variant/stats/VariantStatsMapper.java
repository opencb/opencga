package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsCalculator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.VariantStatsToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Created on 14/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsMapper extends VariantMapper<ImmutableBytesWritable, Put> {

    public static final String COHORT_IDS = "cohort_ids";
    public static final String TAGMAP_PREFIX = "tagmap.";
    private String study;
    private VariantStatisticsCalculator calculator;
    private Map<String, Set<String>> samples;
    private VariantTableHelper helper;
    private StudyMetadata studyMetadata;
    private VariantStatsToHBaseConverter converter;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsFromVariantRowTsvMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        helper = getHelper();
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        studyMetadata = getStudyMetadata();

        boolean overwrite = context.getConfiguration().getBoolean(VariantStorageOptions.STATS_OVERWRITE.key(), false);
        calculator = new VariantStatisticsCalculator(overwrite);
        Aggregation aggregation = getAggregation(studyMetadata, context.getConfiguration());
        if (AggregationUtils.isAggregated(aggregation)) {
            Properties tagmap = getAggregationMappingProperties(context.getConfiguration());
            calculator.setAggregationType(aggregation, tagmap);
        }
        study = studyMetadata.getName();

        Collection<Integer> cohorts = getCohorts(context.getConfiguration());
        Map<String, Integer> cohortIds = new HashMap<>(cohorts.size());
        samples = new HashMap<>(cohorts.size());

        cohorts.forEach(cohortId -> {
            CohortMetadata cohortMetadata = metadataManager.getCohortMetadata(studyMetadata.getId(), cohortId);
            String cohort = cohortMetadata.getName();
            cohortIds.put(cohort, cohortId);

            Set<String> samplesInCohort = cohortMetadata.getSamples().stream()
                    .map(s -> metadataManager.getSampleName(studyMetadata.getId(), s))
                    .collect(Collectors.toSet());
            samples.put(cohort, samplesInCohort);
        });

        converter = new VariantStatsToHBaseConverter(studyMetadata, cohortIds);

    }

    @Override
    protected void map(Object key, Variant variant, Context context) throws IOException, InterruptedException {
        try {
            List<VariantStatsWrapper> variantStatsWrappers = calculator.calculateBatch(Collections.singletonList(variant), study, samples);
            if (variantStatsWrappers.isEmpty()) {
                return;
            }
            VariantStatsWrapper stats = variantStatsWrappers.get(0);
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants").increment(1);

            Put put = converter.convert(stats);

            if (put == null) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put.null").increment(1);
            } else {
                HadoopVariantSearchIndexUtils.addNotSyncStatus(put, GenomeHelper.COLUMN_FAMILY_BYTES);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put").increment(1);
                context.write(new ImmutableBytesWritable(helper.getVariantsTable()), put);
            }
        } catch (Exception e) {
            logger.error("Problem with variant " + variant, e);
            throw e;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (calculator.getSkippedFiles() > 0) {
            logger.warn("Non calculated variant stats: " + calculator.getSkippedFiles());
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.skipped").increment(calculator.getSkippedFiles());
        }
    }

    public static List<Integer> getCohorts(Configuration conf) {
        int[] ints = conf.getInts(COHORT_IDS);
        List<Integer> cohorts = new ArrayList<>(ints.length);
        for (int cohortId : ints) {
            cohorts.add(cohortId);
        }
        if (cohorts.isEmpty()) {
            throw new IllegalArgumentException("Missing cohorts!");
        }
        return cohorts;
    }

    public static void setCohorts(Job job, Collection<Integer> cohorts) {
        if (cohorts.isEmpty()) {
            throw new IllegalArgumentException("Missing cohorts!");
        }
        job.getConfiguration().set(COHORT_IDS, cohorts.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    public static Properties getAggregationMappingProperties(Configuration configuration) {
        Properties tagmap = new Properties();
        configuration.iterator().forEachRemaining(entry -> {
            if (entry.getKey().startsWith(TAGMAP_PREFIX)) {
                tagmap.put(entry.getKey().substring(TAGMAP_PREFIX.length()), entry.getValue());
            }
        });
        if (tagmap.isEmpty()) {
            return null;
        } else {
            return tagmap;
        }
    }

    public static void setAggregationMappingProperties(Configuration conf, Properties tagmap) {
        setAggregationMappingProperties(conf::set, tagmap);
    }

    public static void setAggregationMappingProperties(ObjectMap conf, Properties tagmap) {
        setAggregationMappingProperties(conf::put, tagmap);
    }

    public static void setAggregationMappingProperties(BiConsumer<String, String> f, Properties tagmap) {
        if (tagmap != null) {
            for (Map.Entry<Object, Object> entry : tagmap.entrySet()) {
                f.accept(TAGMAP_PREFIX + entry.getKey(), entry.getValue().toString());
            }
        }
    }

    public static void setAggregation(ObjectMap conf, Aggregation aggregation) {
        conf.put(VariantStorageOptions.STATS_AGGREGATION.key(), aggregation.toString());
    }

    public static Aggregation getAggregation(StudyMetadata studyMetadata, Configuration configuration) {
        return AggregationUtils.valueOf(configuration.get(VariantStorageOptions.STATS_AGGREGATION.key(),
                studyMetadata.getAggregation().name()));
    }

    public static Aggregation getAggregation(Configuration configuration) {
        return AggregationUtils.valueOf(configuration.get(VariantStorageOptions.STATS_AGGREGATION.key(), Aggregation.NONE.name()));
    }
}
