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
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.VariantStatsToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created on 14/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsFromResultMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private String study;
    private Map<String, List<Integer>> samples;
    private VariantTableHelper helper;
    private StudyMetadata studyMetadata;
    private VariantStatsToHBaseConverter converter;
    private Map<String, HBaseVariantStatsCalculator> calculators;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsFromResultMapper.class);
    private Map<String, Integer> cohortIds;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        helper = new VariantTableHelper(context.getConfiguration());
        Collection<Integer> cohorts;
        try (VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(
                new HBaseVariantStorageMetadataDBAdaptorFactory(helper))) {
            studyMetadata = metadataManager.getStudyMetadata(helper.getStudyId());
            study = studyMetadata.getName();


    //        boolean overwrite = context.getConfiguration().getBoolean(VariantStorageEngine.Options.STATS_OVERWRITE.key(), false);
    //        calculator = new VariantStatisticsCalculator(overwrite);
    //        Properties tagmap = getAggregationMappingProperties(context.getConfiguration());
    //        calculator.setAggregationType(studyConfiguration.getAggregation(), tagmap);
            cohorts = VariantStatsMapper.getCohorts(context.getConfiguration());
            cohortIds = new HashMap<>(cohorts.size());
            samples = new HashMap<>(cohorts.size());
            cohorts.forEach(cohortId -> {
                CohortMetadata cohort = metadataManager.getCohortMetadata(studyMetadata.getId(), cohortId);
                cohortIds.put(cohort.getName(), cohortId);
                List<Integer> samplesInCohort = cohort.getSamples();
                samples.put(cohort.getName(), samplesInCohort);
            });
            converter = new VariantStatsToHBaseConverter(studyMetadata, cohortIds);

        }

        calculators = new HashMap<>(cohorts.size());
        String unknownGenotype = context.getConfiguration().get(
                VariantStorageOptions.STATS_DEFAULT_GENOTYPE.key(),
                VariantStorageOptions.STATS_DEFAULT_GENOTYPE.defaultValue());
        boolean statsMultiAllelic = context.getConfiguration().getBoolean(
                VariantStorageOptions.STATS_MULTI_ALLELIC.key(),
                VariantStorageOptions.STATS_MULTI_ALLELIC.defaultValue());
        try (VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(
                new HBaseVariantStorageMetadataDBAdaptorFactory(helper))) {
            samples.forEach((cohort, samples) -> calculators.put(cohort, new HBaseVariantStatsCalculator(
                    metadataManager, studyMetadata, samples, statsMultiAllelic, unknownGenotype)));
        }
    }


    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            while (context.nextKeyValue()) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants").increment(1);
                if (context.getCurrentValue().isPartial()) {
                    // TODO: Allow partial results
//                    mapPartialResult(context.getCurrentKey(), context);
//                    context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "partialVariant").increment(1);
                    throw new IllegalArgumentException("Invalid partial results. Pending.");
                } else {
                    map(context.getCurrentKey(), context.getCurrentValue(), context);
                }
            }
        } finally {
            cleanup(context);
        }
    }

//    protected void mapPartialResult(ImmutableBytesWritable key, Context context) throws IOException, InterruptedException {
//        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(key.get());
//        VariantStatsWrapper wrapper = new VariantStatsWrapper(variant, new HashMap<>(calculators.size()));
//
//        int numPartialResults = 0;
//        Map<String, Map<Genotype, Integer>> gtCountMap = new HashMap<>(calculators.size());
//        while (true) {
//            Result partialResult = context.getCurrentValue();
//            if (!Arrays.equals(partialResult.getRow(), key.get())) {
//                Variant actualVariant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(partialResult.getRow());
//                throw new IllegalArgumentException("Error reading partial results. Non consecutive results. "
//                        + "Expecting " + variant + " \"" + Bytes.toStringBinary(key.get()) + "\" , "
//                        + "but got " + actualVariant + " \"" + Bytes.toStringBinary(partialResult.getRow()) + "\".");
//            }
//            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "partialResult").increment(1);
//            calculators.forEach((cohort, calculator) ->
//                    gtCountMap.compute(cohort, (k, gtCount) -> calculator.convert(partialResult, variant, gtCount)));
//            numPartialResults++;
//            if (!context.getCurrentValue().isPartial()) {
//                // Break loop when finding the last partial
//                break;
//            }
//            if (!context.nextKeyValue()) {
//                break;
//            }
//        }
//
//        String counterName = numPartialResults < 5
//                ? ("partialResultSize_" + numPartialResults)
//                : ("partialResultSize_" + (numPartialResults / 5 * 5) + '-' + (numPartialResults / 5 * 5 + 5));
//        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, counterName).increment(1);
//
//        calculators.forEach((cohort, calculator) -> {
//            VariantStats stats = calculator.calculate(variant, gtCountMap.get(cohort));
//            wrapper.getCohortStats().put(cohort, stats);
//        });
//
//        write(context, wrapper);
//    }

    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(value.getRow());
        VariantStatsWrapper wrapper = new VariantStatsWrapper(variant, new ArrayList<>(calculators.size()));

        calculators.forEach((cohort, calculator) -> {
            VariantStats stats = calculator.apply(value);
            stats.setCohortId(cohort);
            wrapper.getCohortStats().add(stats);
        });

        write(context, wrapper);
    }

    private void write(Context context, VariantStatsWrapper wrapper) throws IOException, InterruptedException {
        Put put = converter.convert(wrapper);
        if (put == null) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put.null").increment(1);
        } else {
            HadoopVariantSearchIndexUtils.addNotSyncStatus(put, GenomeHelper.COLUMN_FAMILY_BYTES);
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put").increment(1);
            context.write(new ImmutableBytesWritable(helper.getVariantsTable()), put);
        }
    }
}
