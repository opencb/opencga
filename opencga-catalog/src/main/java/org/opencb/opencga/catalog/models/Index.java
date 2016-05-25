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
    private IndexStatus status;
    private long jobId;

    private Map<String, Object> attributes;

    public Index() {
    }

    public Index(String userId, String date, IndexStatus status, long jobId, Map<String, Object> attributes) {
        this.userId = userId;
        this.date = date;
        this.status = status;
        this.jobId = jobId;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Index{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", status=").append(status);
        sb.append(", jobId=").append(jobId);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
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

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public IndexStatus getStatus() {
        return status;
    }

    public void setStatus(IndexStatus status) {
        this.status = status;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public static class IndexStatus extends Status {

        /*
         * States
         *
         * NONE --> TRANSFORMING --> TRANSFORMED --> LOADING --> READY
         *      \                                              /
         *       ------------------> INDEXING ----------------/
         *
         */
        public static final String NONE = "NONE";
        public static final String TRANSFORMING = "TRANSFORMING";
        public static final String TRANSFORMED = "TRANSFORMED";
        public static final String LOADING = "LOADING";
        public static final String INDEXING = "INDEXING";

        public IndexStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public IndexStatus(String status) {
            this(status, "");
        }

        public IndexStatus() {
            this(NONE, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (status != null && (status.equals(NONE) || status.equals(TRANSFORMING) || status.equals(TRANSFORMED)
                    || status.equals(LOADING) || status.equals(INDEXING))) {
                return true;
            }
            return false;
        }
    }

}
