package org.opencb.opencga.core.models.common;

import org.opencb.opencga.core.common.TimeUtils;

public class RgaIndex {

    private Status status;
    private String date;

    public enum Status {
        NOT_INDEXED,
        INDEXED,
        INVALID_PERMISSIONS,
        INVALID_METADATA,
        INVALID
    }

    public RgaIndex() {
    }

    public RgaIndex(Status status, String date) {
        this.status = status;
        this.date = date;
    }

    public static RgaIndex init() {
        return new RgaIndex(Status.NOT_INDEXED, TimeUtils.getTime());
    }

    public Status getStatus() {
        return status;
    }

    public RgaIndex setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getDate() {
        return date;
    }

    public RgaIndex setDate(String date) {
        this.date = date;
        return this;
    }
}
