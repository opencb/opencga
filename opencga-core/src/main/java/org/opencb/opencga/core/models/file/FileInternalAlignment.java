package org.opencb.opencga.core.models.file;

public class FileInternalAlignment {

    private FileInternalAlignmentIndex index;

    public FileInternalAlignment() {
    }

    public FileInternalAlignment(FileInternalAlignmentIndex index) {
        this.index = index;
    }

    public static FileInternalAlignment init() {
        return new FileInternalAlignment(FileInternalAlignmentIndex.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalAlignment{");
        sb.append("index=").append(index);
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
}
