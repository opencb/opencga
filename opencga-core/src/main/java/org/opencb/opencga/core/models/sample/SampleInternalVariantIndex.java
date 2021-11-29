package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.InternalStatus;

public class SampleInternalVariantIndex {

    private InternalStatus status;
    private int numFiles;
    private boolean multiFile;

    public SampleInternalVariantIndex() {
    }

    public SampleInternalVariantIndex(InternalStatus status, int numFiles, boolean multiFile) {
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

    public InternalStatus getStatus() {
        return status;
    }

    public SampleInternalVariantIndex setStatus(InternalStatus status) {
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
}
