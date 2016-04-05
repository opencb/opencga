package org.opencb.opencga.catalog.models;

import org.opencb.opencga.core.common.TimeUtils;

/**
 * Created by pfurio on 11/03/16.
 */
public class Status {

    /**
     * ACTIVE status means that the object is being used.
     */
    public static final String ACTIVE = "active";
    /**
     * DELETED status means that the object is marked as deleted although is still available in the database.
     */
    public static final String DELETED = "deleted";
    /**
     * REMOVED status means that the object is marked as removed, so it will get completely removed from the database ASAP.
     */
    public static final String REMOVED = "removed";

    private String status;
    private String date;
    private String message;

    public Status() {
        this(ACTIVE, "");
    }

    public Status(String status) {
        this(status, "");
    }

    public Status(String status, String message) {
        this.status = status;
        this.date = TimeUtils.getTimeMillis();
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDate() {
        return date;
    }

    public void setDate() {
        this.date = TimeUtils.getTimeMillis();
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Status{");
        sb.append("status='").append(status).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
