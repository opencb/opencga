/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.models;

import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */

public class Index {

    private String userId;
    private String date;
    private Status status;
    private int jobId;

    private Map<String, Object> attributes;

    /**
     * States
     *
     * NONE --> TRANSFORMING --> TRANSFORMED --> LOADING --> READY
     *      \                                              /
     *       ------------------> INDEXING ----------------/
     *
     */
    public enum Status {NONE, TRANSFORMING, TRANSFORMED, LOADING, INDEXING, READY}


    public Index() {
    }

    public Index(String userId, String date, Status status, int jobId, Map<String, Object> attributes) {
        this.userId = userId;
        this.date = date;
        this.status = status;
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

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
