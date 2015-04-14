package org.opencb.opencga.catalog.beans;

import java.util.Map;

/**
 * Created by hpccoll1 on 13/04/15.
 */

public class Index {

    private String userId;
    private String date;
//    private Status status;
    private int jobId;

    private Map<String, Object> attributes;

    /**
     * States
     */
    public enum Status {INDEXING, READY}


    public Index() {
    }

    public Index(String userId, String date, int jobId, Map<String, Object> attributes) {
        this.userId = userId;
        this.date = date;
        this.jobId = jobId;
        this.attributes = attributes;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
