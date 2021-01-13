package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

public class SampleFileIndexQuery {

    private final String sampleName;
    private final short fileIndexMask;
    private final RangeQuery qualQuery;
    private final RangeQuery dpQuery;
    private final boolean hasFileIndexMask1;
    private final boolean hasFileIndexMask2;

    private final boolean[] validFileIndex1;
    private final boolean[] validFileIndex2;

    public SampleFileIndexQuery(String sampleName) {
        this.sampleName = sampleName;
        fileIndexMask = IndexUtils.EMPTY_MASK;
        qualQuery = null;
        dpQuery = null;
        validFileIndex1 = new boolean[1 << Byte.SIZE];
        validFileIndex2 = new boolean[1 << Byte.SIZE];
        hasFileIndexMask1 = false;
        hasFileIndexMask2 = false;
    }

    public SampleFileIndexQuery(String sampleName, short fileIndexMask, RangeQuery qualQuery, RangeQuery dpQuery,
                                boolean[] validFileIndex1, boolean[] validFileIndex2) {
        this.sampleName = sampleName;
        this.fileIndexMask = fileIndexMask;
        this.hasFileIndexMask1 = IndexUtils.getByte1(fileIndexMask) != IndexUtils.EMPTY_MASK;
        this.hasFileIndexMask2 = IndexUtils.getByte2(fileIndexMask) != IndexUtils.EMPTY_MASK;
        this.qualQuery = qualQuery;
        this.dpQuery = dpQuery;
        this.validFileIndex1 = validFileIndex1;
        this.validFileIndex2 = validFileIndex2;
    }

    public String getSampleName() {
        return sampleName;
    }

    public short getFileIndexMask() {
        return fileIndexMask;
    }

    public byte getFileIndexMask1() {
        return (byte) IndexUtils.getByte1(fileIndexMask);
    }

    public byte getFileIndexMask2() {
        return (byte) IndexUtils.getByte2(fileIndexMask);
    }

    public boolean hasFileIndexMask1() {
        return hasFileIndexMask1;
    }

    public boolean hasFileIndexMask2() {
        return hasFileIndexMask2;
    }

    public boolean[] getValidFileIndex1() {
        return validFileIndex1;
    }

    public boolean[] getValidFileIndex2() {
        return validFileIndex2;
    }

    public RangeQuery getQualQuery() {
        return qualQuery;
    }

    public RangeQuery getDpQuery() {
        return dpQuery;
    }
}
