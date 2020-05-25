package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.Collections;

public class ChromosomeAccumulator extends CategoricalAccumulator<Variant> {

    public ChromosomeAccumulator() {
        this(null);
    }

    public ChromosomeAccumulator(FieldVariantAccumulator<Variant> nestedFieldAccumulator) {
        super(v -> Collections.singletonList(v.getChromosome()), VariantField.CHROMOSOME.fieldName(), nestedFieldAccumulator);
    }
}
