package org.opencb.opencga.catalog.models;

import org.opencb.opencga.core.common.TimeUtils;

/**
 * Created by pfurio on 11/03/16.
 */
public class Status {

    /**
     * READY status means that the object is being used.
     */
    public static final String READY = "READY";

    /**
     * DELETED status means that the object is marked as deleted although is still available in the database.
     */
    public static final String DELETED = "DELETED";

    /**
     * REMOVED status means that the object is marked as removed, so it will get completely removed from the database ASAP.
     */
    public static final String REMOVED = "REMOVED";

    private String status;
    private String date;
    private String message;

    public Status() {
        this(READY, "");
    }

    public Status(String status) {
        this(status, "");
    }

    public Status(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status '" + status + "'");
        }
    }

    protected void init(String status, String message) {
        this.status = status;
        this.date = TimeUtils.getTime();
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

    public void setDate(String date) {
        this.date = date;
    }

    public void setCurrentDate() {
        this.date = TimeUtils.getTime();
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public static boolean isValid(String status) {
        if (status != null && (status.equals(READY) || status.equals(DELETED) || status.equals(REMOVED))) {
            return true;
        }
        return false;
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
