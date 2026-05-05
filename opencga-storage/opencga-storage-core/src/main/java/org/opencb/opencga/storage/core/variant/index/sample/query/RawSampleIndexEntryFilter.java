package org.opencb.opencga.storage.core.variant.index.sample.query;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntryIterator;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.Comparator;

public class RawSampleIndexEntryFilter extends AbstractSampleIndexEntryFilter<SampleIndexVariant> {

    public RawSampleIndexEntryFilter(SingleSampleIndexQuery query) {
        super(query);
    }

    public RawSampleIndexEntryFilter(SingleSampleIndexQuery query, LocusQuery regions) {
        super(query, regions);
    }

    @Override
    protected SampleIndexVariant getNext(SampleIndexEntryIterator variants) {
        return variants.nextSampleIndexVariant();
    }

    @Override
    protected Variant toVariant(SampleIndexVariant v) {
        return v.getVariant();
    }

    @Override
    protected boolean sameGenomicVariant(SampleIndexVariant v1, SampleIndexVariant v2) {
        return v1.getVariant().sameGenomicVariant(v2.getVariant());
    }

    @Override
    protected Comparator<SampleIndexVariant> getComparator() {
        return Comparator.comparing(SampleIndexVariant::getVariant, SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR);
    }
}
