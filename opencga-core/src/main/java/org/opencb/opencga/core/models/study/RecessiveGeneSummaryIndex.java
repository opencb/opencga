package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.common.TimeUtils;

public class RecessiveGeneSummaryIndex {

    private Status status;
    private String modificationDate;

    public RecessiveGeneSummaryIndex() {
    }

    public RecessiveGeneSummaryIndex(Status status, String modificationDate) {
        this.status = status;
        this.modificationDate = modificationDate;
    }

    public static RecessiveGeneSummaryIndex init() {
        return new RecessiveGeneSummaryIndex(Status.NOT_INDEXED, TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RecessiveGeneSummaryIndex{");
        sb.append("status=").append(status);
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Status getStatus() {
        return status;
    }

    public RecessiveGeneSummaryIndex setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public RecessiveGeneSummaryIndex setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public enum Status {
        NOT_INDEXED,
        INDEXED,
        INVALID
    }
}
