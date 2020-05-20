package org.opencb.opencga.storage.hadoop.variant.analysis.julie;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantRowMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class JulieToolMapper extends VariantRowMapper<ImmutableBytesWritable, Put> {

    private HBaseToVariantAnnotationConverter fromHBaseConverter;
    private VariantAnnotationToHBaseConverter toHBaseConverter;
    private boolean overwrite;
    private Map<Integer, String> studyIdMap;
    private Map<Integer, Map<Integer, String>> cohortIdMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        fromHBaseConverter = new HBaseToVariantAnnotationConverter();
        toHBaseConverter = new VariantAnnotationToHBaseConverter();
        overwrite = context.getConfiguration().getBoolean(JulieToolDriver.OVERWRITE, false);
        try (VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(
                new HBaseVariantStorageMetadataDBAdaptorFactory(getHelper()))) {
            studyIdMap = new HashMap<>();
            for (Map.Entry<Integer, String> entry : metadataManager.getStudies().inverse().entrySet()) {
                String studyName = entry.getValue();
                if (studyName.contains(":")) {
                    studyName = studyName.substring(studyName.lastIndexOf(":") + 1);
                }
                studyIdMap.put(entry.getKey(), studyName);
            }
            cohortIdMap = new HashMap<>();
            for (Integer studyId : studyIdMap.keySet()) {
                HashMap<Integer, String> studyCohorts = new HashMap<>();
                cohortIdMap.put(studyId, studyCohorts);
                metadataManager.cohortIterator(studyId).forEachRemaining(c -> studyCohorts.put(c.getId(), c.getName()));
            }
        }
    }

    @Override
    protected void map(Object key, VariantRow value, Context context) throws IOException, InterruptedException {
        AtomicReference<VariantAnnotation> annotationRef = new AtomicReference<>();
        List<PopulationFrequency> populationFrequencies = new LinkedList<>();

        Variant variant = value.getVariant();

        value.walker()
                .onVariantAnnotation(a -> annotationRef.set(fromHBaseConverter.convert(a.toBytesWritable())))
                .onCohortStats(column -> {
                    VariantProto.VariantStats variantStats = column.toProto();
                    if (variantStats.getAltAlleleFreq() <= 0) {
                        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "skip_stats").increment(1);
                        // Ignore when alternate allele is 0
                        return;
                    }
                    int studyId = column.getStudyId();
                    int cohortId = column.getCohortId();
                    populationFrequencies.add(new PopulationFrequency(
                            studyIdMap.get(studyId),
                            cohortIdMap.get(studyId).get(cohortId),
                            variant.getReference(), variant.getAlternate(),
                            variantStats.getRefAlleleFreq(), variantStats.getAltAlleleFreq(), null, null, null));

                })
                .walk();

        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants").increment(1);
        if (populationFrequencies.isEmpty()) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "empty_stats").increment(1);
        }
        VariantAnnotation annotation = annotationRef.get();
        if (annotation == null) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "empty_annotation").increment(1);
        }

        if (overwrite) {
            if (annotation == null) {
                return;
            }
            annotation.setPopulationFrequencies(populationFrequencies);
        } else {
            if (populationFrequencies.isEmpty() || annotation == null) {
                return;
            }
            if (annotation.getPopulationFrequencies() == null || annotation.getPopulationFrequencies().isEmpty()) {
                annotation.setPopulationFrequencies(populationFrequencies);
            } else {
                for (PopulationFrequency populationFrequency : populationFrequencies) {
                    annotation.getPopulationFrequencies()
                            .removeIf(p -> p.getStudy().equals(populationFrequency.getStudy())
                                    && p.getPopulation().equals(populationFrequency.getPopulation()));
                }
                annotation.getPopulationFrequencies().addAll(populationFrequencies);
            }
        }

        context.write(((ImmutableBytesWritable) key), toHBaseConverter.convert(annotation));

    }
}
