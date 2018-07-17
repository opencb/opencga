package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.VariantStatsToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 14/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsFromResultMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private String study;
    private Map<String, Set<String>> samples;
    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration;
    private VariantStatsToHBaseConverter converter;
    private Map<String, HBaseVariantStatsCalculator> calculators;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsFromResultMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        helper = new VariantTableHelper(context.getConfiguration());
        studyConfiguration = helper.readStudyConfiguration();
//        boolean overwrite = context.getConfiguration().getBoolean(VariantStorageEngine.Options.OVERWRITE_STATS.key(), false);
//        calculator = new VariantStatisticsCalculator(overwrite);
//        Properties tagmap = getAggregationMappingProperties(context.getConfiguration());
//        calculator.setAggregationType(studyConfiguration.getAggregation(), tagmap);
        study = studyConfiguration.getStudyName();
        converter = new VariantStatsToHBaseConverter(helper, studyConfiguration);
        Collection<Integer> cohorts = VariantStatsMapper.getCohorts(context.getConfiguration());
        samples = new HashMap<>(cohorts.size());
        cohorts.forEach(cohortId -> {
            String cohort = studyConfiguration.getCohortIds().inverse().get(cohortId);
            Set<String> samplesInCohort = studyConfiguration.getCohorts().get(cohortId).stream()
                    .map(studyConfiguration.getSampleIds().inverse()::get)
                    .collect(Collectors.toSet());
            samples.put(cohort, samplesInCohort);
        });

        calculators = new HashMap<>(cohorts.size());
        samples.forEach((cohort, samples) -> {
            calculators.put(cohort,
                    new HBaseVariantStatsCalculator(helper.getColumnFamily(), studyConfiguration, new ArrayList<>(samples)));
        });
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants").increment(1);
        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(value.getRow());
        VariantStatsWrapper wrapper = new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), variant.getEnd(),
                new HashMap<>(calculators.size()), null);

        calculators.forEach((cohort, calculator) -> {
            VariantStats stats = calculator.apply(value);
            wrapper.getCohortStats().put(cohort, stats);
        });

        Put put = converter.convert(wrapper);
        if (put == null) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put.null").increment(1);
        } else {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put").increment(1);
            context.write(new ImmutableBytesWritable(helper.getVariantsTable()), put);
        }

    }
}
