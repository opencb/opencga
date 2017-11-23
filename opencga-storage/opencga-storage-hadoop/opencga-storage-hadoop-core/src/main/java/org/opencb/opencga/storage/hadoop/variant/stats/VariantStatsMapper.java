package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsCalculator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.VariantStatsToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 14/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsMapper extends VariantMapper<ImmutableBytesWritable, Put> {

    public static final String COHORTS = "cohorts";
    private String study;
    private VariantStatisticsCalculator calculator;
    private Map<String, Set<String>> samples;
    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration;
    private VariantStatsToHBaseConverter converter;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsMapper.class);


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        calculator = new VariantStatisticsCalculator(true);
        // TODO: Set aggregation
        calculator.setAggregationType(Aggregation.NONE, null);
        helper = new VariantTableHelper(context.getConfiguration());
        studyConfiguration = helper.readStudyConfiguration();
        study = studyConfiguration.getStudyName();
        converter = new VariantStatsToHBaseConverter(helper, studyConfiguration);

        Collection<Integer> cohorts = getCohorts(context.getConfiguration());
        samples = new HashMap<>(cohorts.size());
        cohorts.forEach(cohortId -> {
            String cohort = studyConfiguration.getCohortIds().inverse().get(cohortId);
            Set<String> samplesInCohort = studyConfiguration.getCohorts().get(cohortId).stream()
                    .map(studyConfiguration.getSampleIds().inverse()::get)
                    .collect(Collectors.toSet());
            samples.put(cohort, samplesInCohort);
        });
    }

    @Override
    protected void map(Object key, Variant variant, Context context) throws IOException, InterruptedException {
        try {

            List<VariantStatsWrapper> variantStatsWrappers = calculator.calculateBatch(Collections.singletonList(variant), study, samples);
            if (variantStatsWrappers.isEmpty()) {
                return;
            }
            VariantStatsWrapper stats = variantStatsWrappers.get(0);

            context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "variants").increment(1);

            Put put = converter.convert(stats);
            if (put == null) {
                System.out.println("PUT NULL FOR VARIANT " + variant);
            } else {
                context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put").increment(1);
                context.write(new ImmutableBytesWritable(helper.getAnalysisTable()), put);
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
            context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.skipped").increment(calculator.getSkippedFiles());
        }
    }

    public static Collection<Integer> getCohorts(Configuration conf) {
        int[] ints = conf.getInts(COHORTS);
        List<Integer> cohorts = new ArrayList<>(ints.length);
        for (int cohortId : ints) {
            cohorts.add(cohortId);
        }
        return cohorts;
    }

    public static void setCohorts(Job job, Collection<Integer> cohorts) {
        job.getConfiguration().set(COHORTS, cohorts.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
