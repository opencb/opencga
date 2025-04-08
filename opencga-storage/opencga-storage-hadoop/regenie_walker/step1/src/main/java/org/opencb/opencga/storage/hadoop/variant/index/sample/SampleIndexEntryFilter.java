package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.query.LocusQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;

import java.util.Comparator;

public class SampleIndexEntryFilter extends AbstractSampleIndexEntryFilter<Variant> {

    public SampleIndexEntryFilter(SingleSampleIndexQuery query) {
        super(query);
    }

    public SampleIndexEntryFilter(SingleSampleIndexQuery query, LocusQuery locusQuery) {
        super(query, locusQuery);
    }

    @Override
    protected Variant getNext(SampleIndexEntryIterator variants) {
        return variants.next();
    }

    @Override
    protected Variant toVariant(Variant v) {
        return v;
    }

    @Override
    protected boolean sameGenomicVariant(Variant v1, Variant v2) {
        return v1.sameGenomicVariant(v2);
    }

    @Override
    protected Comparator<Variant> getComparator() {
        return SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;
    }
}
