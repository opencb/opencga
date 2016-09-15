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

import java.util.Collections;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileIndex {

    @Deprecated
    private String userId;
    private String creationDate;
    private IndexStatus status;

    @Deprecated
    private long jobId;

    private TransformedFile transformedFile;
    private LocalFileIndex localFileIndex;

    private Map<String, Object> attributes;


    public FileIndex() {
        this(null, null, null, -1, Collections.emptyMap());
    }

    public FileIndex(String userId, String creationDate, IndexStatus status, long jobId, Map<String, Object> attributes) {
        this(userId, creationDate, status, jobId, null, null, attributes);
    }

    public FileIndex(String userId, String creationDate, IndexStatus status, long jobId, TransformedFile transformedFile,
                     LocalFileIndex localFileIndex, Map<String, Object> attributes) {
        this.userId = userId;
        this.creationDate = creationDate;
        this.status = status != null ? status : new IndexStatus(IndexStatus.NONE);
        this.jobId = jobId;
        this.transformedFile = transformedFile;
        this.localFileIndex = localFileIndex;
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

    public static class TransformedFile {
        private long id;
        private long metadataId;

        public TransformedFile() {
        }

        public TransformedFile(long id, long metadataId) {
            this.id = id;
            this.metadataId = metadataId;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("TransformedFile{");
            sb.append("id=").append(id);
            sb.append(", metadataId=").append(metadataId);
            sb.append('}');
            return sb.toString();
        }

        public long getId() {
            return id;
        }

        public TransformedFile setId(long id) {
            this.id = id;
            return this;
        }

        public long getMetadataId() {
            return metadataId;
        }

        public TransformedFile setMetadataId(long metadataId) {
            this.metadataId = metadataId;
            return this;
        }
    }

    public class LocalFileIndex {

        private long fileId;
        // This typically will be: tabix, sammtools, ...
        private String indexer;

        public LocalFileIndex(long fileId, String indexer) {
            this.fileId = fileId;
            this.indexer = indexer;
        }


        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("LocalFileIndex{");
            sb.append("fileId=").append(fileId);
            sb.append(", indexer='").append(indexer).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public long getFileId() {
            return fileId;
        }

        public LocalFileIndex setFileId(long fileId) {
            this.fileId = fileId;
            return this;
        }

        public String getIndexer() {
            return indexer;
        }

        public LocalFileIndex setIndexer(String indexer) {
            this.indexer = indexer;
            return this;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileIndex{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", jobId=").append(jobId);
        sb.append(", transformedFile=").append(transformedFile);
        sb.append(", localFileIndex=").append(localFileIndex);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getUserId() {
        return userId;
    }

    public FileIndex setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public FileIndex setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public IndexStatus getStatus() {
        return status;
    }

    public FileIndex setStatus(IndexStatus status) {
        this.status = status;
        return this;
    }

//    public FileIndex setStatus(String status) {
//        if (IndexStatus.isValid(status)) {
//            this.status.setName(status);
//            this.status.setCurrentDate();
//        }
//        return this;
//    }

    public long getJobId() {
        return jobId;
    }

    public FileIndex setJobId(long jobId) {
        this.jobId = jobId;
        return this;
    }

    public TransformedFile getTransformedFile() {
        return transformedFile;
    }

    public FileIndex setTransformedFile(TransformedFile transformedFile) {
        this.transformedFile = transformedFile;
        return this;
    }

    public LocalFileIndex getLocalFileIndex() {
        return localFileIndex;
    }

    public FileIndex setLocalFileIndex(LocalFileIndex localFileIndex) {
        this.localFileIndex = localFileIndex;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public FileIndex setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
