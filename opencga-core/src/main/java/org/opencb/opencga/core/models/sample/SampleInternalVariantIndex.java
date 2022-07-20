package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class SampleInternalVariantIndex {

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private IndexStatus status;
    @DataField(description = ParamConstants.SAMPLE_INTERNAL_VARIANT_INDEX_NUM_FILES_DESCRIPTION)
    private int numFiles;
    @DataField(description = ParamConstants.SAMPLE_INTERNAL_VARIANT_INDEX_MULTI_FILE_DESCRIPTION)
    private boolean multiFile;

    public SampleInternalVariantIndex() {
    }

    public SampleInternalVariantIndex(IndexStatus status, int numFiles, boolean multiFile) {
        this.status = status;
        this.numFiles = numFiles;
        this.multiFile = multiFile;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariantIndex{");
        sb.append("status=").append(status);
        sb.append(", numFiles=").append(numFiles);
        sb.append(", multiFile=").append(multiFile);
        sb.append('}');
        return sb.toString();
    }

    public IndexStatus getStatus() {
        return status;
    }

    public SampleInternalVariantIndex setStatus(IndexStatus status) {
        this.status = status;
        return this;
    }

    public int getNumFiles() {
        return numFiles;
    }

    public SampleInternalVariantIndex setNumFiles(int numFiles) {
        this.numFiles = numFiles;
        return this;
    }

    public boolean isMultiFile() {
        return multiFile;
    }

    public SampleInternalVariantIndex setMultiFile(boolean multiFile) {
        this.multiFile = multiFile;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleInternalVariantIndex that = (SampleInternalVariantIndex) o;
        return numFiles == that.numFiles &&
                multiFile == that.multiFile &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, numFiles, multiFile);
    }
}
