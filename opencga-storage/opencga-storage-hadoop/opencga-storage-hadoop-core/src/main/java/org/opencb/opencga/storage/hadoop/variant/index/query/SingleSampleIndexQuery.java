package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.opencb.opencga.storage.core.variant.query.Values;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class SingleSampleIndexQuery extends SampleIndexQuery {

    private final String sample;
    private final List<String> gts;
    private final Values<SampleFileIndexQuery> sampleFileIndexQuery;
    private final boolean[] fatherFilter;
    private final boolean[] motherFilter;
    private final boolean mendelianError;
    private final boolean multiFileSample;
    private final boolean emptyFileIndex;

    protected SingleSampleIndexQuery(SampleIndexQuery query, String sample) {
        this(query, sample, query.getSamplesMap().get(sample));
    }

    protected SingleSampleIndexQuery(SampleIndexQuery query, String sample, List<String> gts) {
        super(query.getSchema(), query.getLocusQueries() == null ? null : new ArrayList<>(query.getLocusQueries()),
                query.getVariantTypes() == null ? null : new HashSet<>(query.getVariantTypes()),
                query.getStudy(),
                Collections.singletonMap(sample, gts),
                query.getMultiFileSamplesSet(),
                query.getNegatedSamples(),
                query.getFatherFilterMap(),
                query.getMotherFilterMap(),
                query.getSampleFileIndexQueryMap(),
                query.getAnnotationIndexQuery(),
                query.getMendelianErrorSet(),
                query.getMendelianErrorType(),
                query.getQueryOperation());
        this.sample = sample;
        this.gts = gts;
        multiFileSample = query.getMultiFileSamplesSet().contains(sample);
        fatherFilter = getFatherFilter(sample);
        motherFilter = getMotherFilter(sample);
        sampleFileIndexQuery = getSampleFileIndexQuery(sample);
        mendelianError = query.getMendelianErrorSet().contains(sample);
        emptyFileIndex = sampleFileIndexQuery.isEmpty() || sampleFileIndexQuery.stream().allMatch(SampleFileIndexQuery::isEmpty);
    }

    @Override
    public boolean emptyFileIndex() {
        return emptyFileIndex;
    }

    public String getSample() {
        return sample;
    }

    public List<String> getGenotypes() {
        return gts;
    }

    public boolean isNegated() {
        return super.isNegated(sample);
    }

    public boolean isMultiFileSample() {
        return multiFileSample;
    }

    public boolean[] getFatherFilter() {
        return fatherFilter;
    }

    public boolean hasFatherFilter() {
        return fatherFilter != EMPTY_PARENT_FILTER;
    }

    public boolean[] getMotherFilter() {
        return motherFilter;
    }

    public boolean hasMotherFilter() {
        return motherFilter != EMPTY_PARENT_FILTER;
    }

    public Values<SampleFileIndexQuery> getSampleFileIndexQuery() {
        return sampleFileIndexQuery;
    }

    public boolean getMendelianError() {
        return mendelianError;
    }
}
