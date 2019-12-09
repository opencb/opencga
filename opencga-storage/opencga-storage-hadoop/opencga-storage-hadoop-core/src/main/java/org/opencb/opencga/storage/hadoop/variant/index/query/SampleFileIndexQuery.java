package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

public class SampleFileIndexQuery {

    private final String sampleName;
    private final byte fileIndexMask;
    private final RangeQuery qualQuery;
    private final RangeQuery dpQuery;

    private final boolean[] validFileIndex;

    public SampleFileIndexQuery(String sampleName) {
        this.sampleName = sampleName;
        fileIndexMask = IndexUtils.EMPTY_MASK;
        qualQuery = null;
        dpQuery = null;
        validFileIndex = new boolean[1 << Byte.SIZE];
    }

    public SampleFileIndexQuery(String sampleName, byte fileIndexMask, RangeQuery qualQuery, RangeQuery dpQuery, boolean[] validFileIndex) {
        this.sampleName = sampleName;
        this.fileIndexMask = fileIndexMask;
        this.qualQuery = qualQuery;
        this.dpQuery = dpQuery;
        this.validFileIndex = validFileIndex;
    }

    public String getSampleName() {
        return sampleName;
    }

    public byte getFileIndexMask() {
        return fileIndexMask;
    }

    public boolean[] getValidFileIndex() {
        return validFileIndex;
    }

    public RangeQuery getQualQuery() {
        return qualQuery;
    }

    public RangeQuery getDpQuery() {
        return dpQuery;
    }
}
