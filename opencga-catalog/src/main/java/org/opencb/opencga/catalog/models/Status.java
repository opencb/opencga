package org.opencb.opencga.catalog.models;

import org.opencb.opencga.core.common.TimeUtils;

/**
 * Created by pfurio on 11/03/16.
 */
public class Status {

    /**
     * READY name means that the object is being used.
     */
    public static final String READY = "READY";

    /**
     * TRASHED name means that the object is marked as deleted although is still available in the database.
     */
    public static final String TRASHED = "TRASHED";

    /**
     * DELETED name means that the object is marked as removed, so it will get completely removed from the database ASAP.
     */
    public static final String DELETED = "DELETED";

    private String name;
    private String date;
    private String message;

    public Status() {
        this(READY, "");
    }

    public Status(String name) {
        this(name, "");
    }

    public Status(String name, String message) {
        if (isValid(name)) {
            init(name, message);
        } else {
            throw new IllegalArgumentException("Unknown name '" + name + "'");
        }
    }

    protected void init(String status, String message) {
        this.name = status;
        this.date = TimeUtils.getTime();
        this.message = message;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        if (status != null && (status.equals(READY) || status.equals(TRASHED) || status.equals(DELETED))) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Status{");
        sb.append("name='").append(name).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
