package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.converters.avro.VariantStatsToTsvConverter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantRowMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;

import java.io.IOException;
import java.util.*;

public class VariantStatsFromVariantRowTsvMapper extends VariantRowMapper<NullWritable, Text> {

    private Map<String, HBaseVariantStatsCalculator> calculators;
    private List<Integer> cohorts;
    private VariantStatsToTsvConverter converter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        cohorts = VariantStatsMapper.getCohorts(context.getConfiguration());
        calculators = new LinkedHashMap<>(cohorts.size());

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        StudyMetadata studyMetadata = getStudyMetadata();

        String unknownGenotype = context.getConfiguration().get(
                VariantStorageOptions.STATS_DEFAULT_GENOTYPE.key(),
                VariantStorageOptions.STATS_DEFAULT_GENOTYPE.defaultValue());
        boolean statsMultiAllelic = context.getConfiguration().getBoolean(
                VariantStorageOptions.STATS_MULTI_ALLELIC.key(),
                VariantStorageOptions.STATS_MULTI_ALLELIC.defaultValue());
        for (Integer cohort : cohorts) {
            CohortMetadata cohortMetadata = metadataManager.getCohortMetadata(studyMetadata.getId(), cohort);
            String alias = cohortMetadata.getAttributes().getString("alias");
            String name = cohortMetadata.getName();
            calculators.put(StringUtils.isNotEmpty(alias) ? alias : name, new HBaseVariantStatsCalculator(
                    metadataManager, studyMetadata, cohortMetadata.getSamples(), statsMultiAllelic, unknownGenotype));
        }

        converter = new VariantStatsToTsvConverter(studyMetadata.getName(), new ArrayList<>(calculators.keySet()));
        context.write(NullWritable.get(), new Text(converter.createHeader()));
    }

    @Override
    protected void map(Object key, VariantRow result, Context context) throws IOException, InterruptedException {
        Variant variant = result.getVariant();
        VariantAnnotation variantAnnotation = result.getVariantAnnotation();
        Map<String, VariantStats> statsMap = new HashMap<>(cohorts.size());
        for (Map.Entry<String, HBaseVariantStatsCalculator> entry : calculators.entrySet()) {
            String cohort = entry.getKey();
            HBaseVariantStatsCalculator calculator = entry.getValue();
            statsMap.put(cohort, calculator.apply(result));
        }
        context.write(NullWritable.get(), new Text(converter.convert(variant, statsMap, variantAnnotation)));
    }

}
