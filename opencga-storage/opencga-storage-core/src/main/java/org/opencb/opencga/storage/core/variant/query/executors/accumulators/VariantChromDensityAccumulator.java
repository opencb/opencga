package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

public class VariantChromDensityAccumulator<T> extends ChromDensityAccumulator<Variant> {

    public VariantChromDensityAccumulator(VariantStorageMetadataManager metadataManager, Region region,
                                          FacetFieldAccumulator<Variant> nestedFieldAccumulator, int step) {
        super(metadataManager, region, nestedFieldAccumulator, step, Variant::getStart);
    }
}
