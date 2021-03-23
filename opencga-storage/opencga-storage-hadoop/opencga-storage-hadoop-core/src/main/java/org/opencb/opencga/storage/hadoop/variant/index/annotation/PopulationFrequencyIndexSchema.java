package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.hadoop.variant.index.core.IndexSchema;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.RangeIndexField;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration.PopulationFrequencyRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PopulationFrequencyIndexSchema extends IndexSchema {

    private final Map<String, IndexField<Double>> populations;

    public PopulationFrequencyIndexSchema(List<PopulationFrequencyRange> populationRanges) {
        fields = new ArrayList<>(populationRanges.size());
        populations = new HashMap<>();
        int bitOffset = 0;
        for (PopulationFrequencyRange configuration : populationRanges) {
            IndexField<Double> field;
            switch (configuration.getType()) {
                case RANGE:
                    field = new RangeIndexField(configuration, bitOffset, configuration.getThresholds());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown index type '" + configuration.getType() + "'");
            }
            bitOffset += field.getBitLength();
            fields.add(field);
            populations.put(configuration.getStudyAndPopulation(), field);
        }
        updateIndexSizeBits();
    }

    public IndexField<Double> getField(String study, String population) {
        return populations.get(study + ":" + population);
    }
}
