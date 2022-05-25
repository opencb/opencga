package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.query.LocusQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;

import java.util.Comparator;

public class RawSampleIndexEntryFilter extends AbstractSampleIndexEntryFilter<SampleVariantIndexEntry> {

    public RawSampleIndexEntryFilter(SingleSampleIndexQuery query) {
        super(query);
    }

    public RawSampleIndexEntryFilter(SingleSampleIndexQuery query, LocusQuery regions) {
        super(query, regions);
    }

    @Override
    protected SampleVariantIndexEntry getNext(SampleIndexEntryIterator variants) {
        return variants.nextSampleVariantIndexEntry();
    }

    @Override
    protected Variant toVariant(SampleVariantIndexEntry v) {
        return v.getVariant();
    }

    @Override
    protected boolean sameGenomicVariant(SampleVariantIndexEntry v1, SampleVariantIndexEntry v2) {
        return v1.getVariant().sameGenomicVariant(v2.getVariant());
    }

    @Override
    protected Comparator<SampleVariantIndexEntry> getComparator() {
        return Comparator.comparing(SampleVariantIndexEntry::getVariant, SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR);
    }
}
