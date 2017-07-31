/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.metadata;

import java.util.*;

/**
 * Batch file operation information.
 *
 * Information about some operation with a set of files
 * Includes the list of processed files, the timestamp used for the HBase Puts and the current status.
 *
 *  Created on 11/03/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class BatchFileOperation {

    public enum Status {
        RUNNING,
        DONE,       // Finished, but some work still needed (optional)
        READY,
        ERROR
    }

    public enum Type {
        LOAD,
        REMOVE,
        OTHER
    }

    private String operationName;
    private List<Integer> fileIds;
    private long timestamp;
    private final TreeMap<Date, Status> status = new TreeMap<>(Date::compareTo);
    private Type type = Type.OTHER;

    public BatchFileOperation() {
    }

    public BatchFileOperation(String operationName, List<Integer> fileIds, long timestamp, Type type) {
        this.operationName = operationName;
        this.fileIds = fileIds;
        this.timestamp = timestamp;
        this.type = type;
    }

    public BatchFileOperation(BatchFileOperation batch) {
        this.operationName = batch.operationName;
        this.fileIds = new ArrayList<>(batch.fileIds);
        this.timestamp = batch.timestamp;
        this.status.putAll(batch.status);
        this.type = batch.type;
    }

    public boolean sameOperation(Collection<Integer> fileIds, Type type, String jobOperationName) {
        return this.type.equals(type)
                && this.operationName.equals(jobOperationName)
                && fileIds.size() == this.fileIds.size()
                && fileIds.containsAll(this.fileIds);
    }

    public Status currentStatus() {
        return status.isEmpty() ? null : status.lastEntry().getValue();
    }

    public String getOperationName() {
        return operationName;
    }

    public BatchFileOperation setOperationName(String operationName) {
        this.operationName = operationName;
        return this;
    }

    public List<Integer> getFileIds() {
        return fileIds;
    }

    public BatchFileOperation setFileIds(List<Integer> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public BatchFileOperation setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public TreeMap<Date, Status> getStatus() {
        return status;
    }

    public BatchFileOperation addStatus(Status status) {
        return addStatus(Calendar.getInstance().getTime(), status);
    }

    public BatchFileOperation addStatus(Date date, Status status) {
        this.status.put(date, status);
        return this;
    }

    public Type getType() {
        return type;
    }

    public BatchFileOperation setType(Type type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return "BatchFileOperation{"
                + "operationName='" + operationName + '\''
                + ", fileIds=" + fileIds
                + ", timestamp=" + timestamp
                + ", status=" + status
                + '}';
    }
}
