package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.FixedSizeIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.RangeIndexField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PopulationFrequencyIndexSchema extends FixedSizeIndexSchema {

    private final Map<String, IndexField<Double>> populations;
    private final SampleIndexConfiguration.PopulationFrequencyIndexConfiguration populationFrequencyIndexConfiguration;

    public PopulationFrequencyIndexSchema(SampleIndexConfiguration.PopulationFrequencyIndexConfiguration
                                                  populationFrequencyIndexConfiguration) {
        fields = new ArrayList<>(populationFrequencyIndexConfiguration.getPopulations().size());
        this.populationFrequencyIndexConfiguration = populationFrequencyIndexConfiguration;
        populations = new HashMap<>();
        int bitOffset = 0;
        for (IndexFieldConfiguration configuration : populationFrequencyIndexConfiguration.toIndexFieldConfiguration()) {
            IndexField<Double> field;
            switch (configuration.getType()) {
                case RANGE_LT:
                case RANGE_GT:
                    field = new RangeIndexField(configuration, bitOffset);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown index type '" + configuration.getType() + "'");
            }
            bitOffset += field.getBitLength();
            fields.add(field);
            populations.put(configuration.getKey(), field);
        }
        updateIndexSizeBits();
    }

    public IndexField<Double> getField(String study, String population) {
        return getField(study + ":" + population);
    }

    public IndexField<Double> getField(String studyPop) {
        return populations.get(studyPop);
    }
}
