package org.opencb.opencga.core.models.file;

import java.util.Objects;

public class FileInternalAlignment {

    private FileInternalAlignmentIndex index;
    private FileInternalCoverageIndex coverage;

    public FileInternalAlignment() {
    }

    public FileInternalAlignment(FileInternalAlignmentIndex index, FileInternalCoverageIndex coverage) {
        this.index = index;
        this.coverage = coverage;
    }

    public static FileInternalAlignment init() {
        return new FileInternalAlignment(FileInternalAlignmentIndex.init(), FileInternalCoverageIndex.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalAlignment{");
        sb.append("index=").append(index);
        sb.append(", coverage=").append(coverage);
        sb.append('}');
        return sb.toString();
    }

    public FileInternalAlignmentIndex getIndex() {
        return index;
    }

    public FileInternalAlignment setIndex(FileInternalAlignmentIndex index) {
        this.index = index;
        return this;
    }

    public FileInternalCoverageIndex getCoverage() {
        return coverage;
    }

    public FileInternalAlignment setCoverage(FileInternalCoverageIndex coverage) {
        this.coverage = coverage;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInternalAlignment that = (FileInternalAlignment) o;
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
}
