package org.opencb.opencga.core.models.common;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Objects;

public class RgaIndex {

    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     */
    private Status status;
    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     */
    private String date;

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

    public enum Status {
        NOT_INDEXED,
        INDEXED,
        INVALID_PERMISSIONS,
        INVALID_METADATA,
        INVALID
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RgaIndex rgaIndex = (RgaIndex) o;
        return status == rgaIndex.status && Objects.equals(date, rgaIndex.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, date);
    }
}
