package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.VariantStatsToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;
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
    private StudyMetadata studyMetadata;
    private VariantStatsToHBaseConverter converter;
    private Map<String, HBaseVariantStatsCalculator> calculators;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsFromResultMapper.class);
    private Collection<Integer> cohorts;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        helper = new VariantTableHelper(context.getConfiguration());
        try (VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(
                new HBaseVariantStorageMetadataDBAdaptorFactory(helper))) {
            studyMetadata = metadataManager.getStudyMetadata(helper.getStudyId());
            study = studyMetadata.getName();


    //        boolean overwrite = context.getConfiguration().getBoolean(VariantStorageEngine.Options.OVERWRITE_STATS.key(), false);
    //        calculator = new VariantStatisticsCalculator(overwrite);
    //        Properties tagmap = getAggregationMappingProperties(context.getConfiguration());
    //        calculator.setAggregationType(studyConfiguration.getAggregation(), tagmap);
            cohorts = VariantStatsMapper.getCohorts(context.getConfiguration());
            Map<String, Integer> cohortIds = new HashMap<>(cohorts.size());
            samples = new HashMap<>(cohorts.size());
            cohorts.forEach(cohortId -> {
                CohortMetadata cohort = metadataManager.getCohortMetadata(studyMetadata.getId(), cohortId);
                cohortIds.put(cohort.getName(), cohortId);
                Set<String> samplesInCohort = cohort.getSamples().stream()
                        .map(s -> metadataManager.getSampleName(studyMetadata.getId(), s))
                        .collect(Collectors.toSet());
                samples.put(cohort.getName(), samplesInCohort);
            });
            converter = new VariantStatsToHBaseConverter(helper, studyMetadata, cohortIds);

        }
        calculators = new HashMap<>(cohorts.size());
        try (VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(
                new HBaseVariantStorageMetadataDBAdaptorFactory(helper))) {
            samples.forEach((cohort, samples) -> {
                calculators.put(cohort,
                        new HBaseVariantStatsCalculator(
                                helper.getColumnFamily(), metadataManager, studyMetadata, new ArrayList<>(samples)));
            });
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants").increment(1);
        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(value.getRow());
        VariantStatsWrapper wrapper = new VariantStatsWrapper(variant, new HashMap<>(calculators.size()));

        calculators.forEach((cohort, calculator) -> {
            VariantStats stats = calculator.apply(value);
            wrapper.getCohortStats().put(cohort, stats);
        });

        Put put = converter.convert(wrapper);
        if (put == null) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put.null").increment(1);
        } else {
            HadoopVariantSearchIndexUtils.addNotSyncStatus(put, helper.getColumnFamily());
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put").increment(1);
            context.write(new ImmutableBytesWritable(helper.getVariantsTable()), put);
        }

    }
}
