package org.opencb.opencga.storage.core.variant.index.sample.query;

import org.opencb.opencga.storage.core.variant.index.core.IndexUtils;
import org.opencb.opencga.storage.core.variant.index.core.CombinationTripleIndexSchema;
import org.opencb.opencga.storage.core.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.core.variant.index.core.filters.IndexFilter;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

public class SampleAnnotationIndexQuery {
    private final byte[] annotationIndexMask; // byte[] = {mask , index}
    private final Boolean intergenic;
    private final IndexFieldFilter consequenceTypeFilter;
    private final IndexFieldFilter biotypeFilter;
    private final IndexFieldFilter transcriptFlagFilter;
    private final CombinationTripleIndexSchema.Filter ctBtTfFilter;
    private final IndexFilter clinicalFilter;
    private final IndexFilter populationFrequencyFilter;

    public SampleAnnotationIndexQuery(SampleIndexSchema schema) {
        this.annotationIndexMask = new byte[]{0, 0};
        this.intergenic = null;
        this.consequenceTypeFilter = schema.getCtIndex().getField().noOpFilter();
        this.biotypeFilter = schema.getBiotypeIndex().getField().noOpFilter();
        this.transcriptFlagFilter = schema.getTranscriptFlagIndexSchema().getField().noOpFilter();
        this.ctBtTfFilter = schema.getCtBtTfIndex().getField().noOpFilter();
        this.clinicalFilter = schema.getClinicalIndexSchema().noOpFilter();
        this.populationFrequencyFilter = schema.getPopFreqIndex().noOpFilter();
    }

    public SampleAnnotationIndexQuery(byte[] annotationIndexMask,
                                      Boolean intergenic,
                                      IndexFieldFilter consequenceTypeFilter,
                                      IndexFieldFilter biotypeFilter,
                                      IndexFieldFilter transcriptFlagFilter,
                                      CombinationTripleIndexSchema.Filter ctBtTfFilter,
                                      IndexFilter clinicalFilter,
                                      IndexFilter populationFrequencyFilter) {
        this.annotationIndexMask = annotationIndexMask;
        this.intergenic = intergenic;
        this.consequenceTypeFilter = consequenceTypeFilter;
        this.biotypeFilter = biotypeFilter;
        this.transcriptFlagFilter = transcriptFlagFilter;
        this.ctBtTfFilter = ctBtTfFilter;
        this.clinicalFilter = clinicalFilter;
        this.populationFrequencyFilter = populationFrequencyFilter;
    }

    public byte getAnnotationIndexMask() {
        return annotationIndexMask[0];
    }

    public byte getAnnotationIndex() {
        return annotationIndexMask[1];
    }

    public Boolean getIntergenic() {
        return intergenic;
    }

    public IndexFieldFilter getConsequenceTypeFilter() {
        return consequenceTypeFilter;
    }

    public IndexFieldFilter getBiotypeFilter() {
        return biotypeFilter;
    }

    public IndexFieldFilter getTranscriptFlagFilter() {
        return transcriptFlagFilter;
    }

    public CombinationTripleIndexSchema.Filter getCtBtTfFilter() {
        return ctBtTfFilter;
    }

    public IndexFilter getPopulationFrequencyFilter() {
        return populationFrequencyFilter;
    }

    public IndexFilter getClinicalFilter() {
        return clinicalFilter;
    }

    public boolean isEmpty() {
        return getAnnotationIndexMask() == IndexUtils.EMPTY_MASK
                && biotypeFilter.isNoOp()
                && consequenceTypeFilter.isNoOp()
                && clinicalFilter.isNoOp()
                && populationFrequencyFilter.isNoOp();
    }
}
