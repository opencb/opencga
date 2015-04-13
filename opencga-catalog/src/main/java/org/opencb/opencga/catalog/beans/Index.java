package org.opencb.opencga.catalog.beans;

import java.util.Map;

/**
 * Created by hpccoll1 on 13/04/15.
 */

public class Index {

    public Index() {
    }

    public Index(String userId, String storageEngine, int jobId, Map<String, Object> credentials, Map<String, Object> attributes) {
        this.userId = userId;
        this.storageEngine = storageEngine;
        this.jobId = jobId;
        this.credentials = credentials;
        this.attributes = attributes;
    }

    private String userId;
//    private Status status;
    private String storageEngine;
    private int jobId;

    private Map<String, Object> credentials;
    private Map<String, Object> attributes;

    /**
     * States
     */
    public enum Status {INDEXING, READY}


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public Map<String, Object> getCredentials() {
        return credentials;
    }

    public void setCredentials(Map<String, Object> credentials) {
        this.credentials = credentials;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
