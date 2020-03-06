package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.EMPTY_MASK;

public class SingleSampleIndexQuery extends SampleIndexQuery {

    private final String sample;
    private final List<String> gts;
    private final SampleFileIndexQuery sampleFileIndexQuery;
    private final boolean[] fatherFilter;
    private final boolean[] motherFilter;
    private final boolean mendelianError;

    protected SingleSampleIndexQuery(SampleIndexQuery query, String sample) {
        this(query, sample, query.getSamplesMap().get(sample));
    }

    protected SingleSampleIndexQuery(SampleIndexQuery query, String sample, List<String> gts) {
        super(query.getRegions() == null ? null : new ArrayList<>(query.getRegions()),
                query.getVariantTypes() == null ? null : new HashSet<>(query.getVariantTypes()),
                query.getStudy(),
                Collections.singletonMap(sample, gts),
                query.getNegatedSamples(),
                query.getFatherFilterMap(),
                query.getMotherFilterMap(),
                query.getSampleFileIndexQueryMap(),
                query.getAnnotationIndexQuery(),
                query.getMendelianErrorSet(),
                query.isOnlyDeNovo(),
                query.getQueryOperation());
        this.sample = sample;
        this.gts = gts;
        fatherFilter = getFatherFilter(sample);
        motherFilter = getMotherFilter(sample);
        sampleFileIndexQuery = getSampleFileIndexQuery(sample);
        mendelianError = query.getMendelianErrorSet().contains(sample);
    }

    @Override
    public boolean emptyFileIndex() {
        return sampleFileIndexQuery.getFileIndexMask() == EMPTY_MASK;
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

    public short getFileIndexMask() {
        return sampleFileIndexQuery.getFileIndexMask();
    }

    public byte getFileIndexMask1() {
        return (byte) IndexUtils.getByte1(sampleFileIndexQuery.getFileIndexMask());
    }

    public byte getFileIndexMask2() {
        return (byte) IndexUtils.getByte2(sampleFileIndexQuery.getFileIndexMask());
    }

    public SampleFileIndexQuery getSampleFileIndexQuery() {
        return sampleFileIndexQuery;
    }

    public boolean getMendelianError() {
        return mendelianError;
    }
}
