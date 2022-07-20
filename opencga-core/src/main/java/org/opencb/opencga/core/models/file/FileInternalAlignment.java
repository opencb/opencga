package org.opencb.opencga.core.models.file;

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileInternalAlignment {

    @DataField(description = ParamConstants.FILE_INTERNAL_ALIGNMENT_INDEX_DESCRIPTION)
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
