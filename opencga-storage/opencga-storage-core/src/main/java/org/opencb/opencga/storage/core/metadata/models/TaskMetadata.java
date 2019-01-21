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

package org.opencb.opencga.storage.core.metadata.models;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

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
public class TaskMetadata {

    public enum Status {
        NONE,
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

    private int studyId;
    private int id;
    private String operationName;
    private List<Integer> fileIds;
    private long timestamp;
    private final TreeMap<Date, Status> status = new TreeMap<>(Date::compareTo);
    private Type type = Type.OTHER;

    public TaskMetadata() {
    }

    @Deprecated
    public TaskMetadata(String operationName, List<Integer> fileIds, long timestamp, Type type) {
        this(RandomUtils.nextInt(1, 1000), operationName, fileIds, timestamp, type);
    }

    public TaskMetadata(int id, String operationName, List<Integer> fileIds, long timestamp, Type type) {
        this.id = id;
        this.operationName = operationName;
        this.fileIds = fileIds;
        this.timestamp = timestamp;
        this.type = type;
    }

    public TaskMetadata(TaskMetadata batch) {
        this();
        this.id = batch.id;
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

    public TaskMetadata setOperationName(String operationName) {
        this.operationName = operationName;
        return this;
    }

    public int getStudyId() {
        return studyId;
    }

    public TaskMetadata setStudyId(int studyId) {
        this.studyId = studyId;
        return this;
    }

    public int getId() {
        return id;
    }

    public TaskMetadata setId(int id) {
        this.id = id;
        return this;
    }

    public List<Integer> getFileIds() {
        return fileIds;
    }

    public TaskMetadata setFileIds(List<Integer> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TaskMetadata setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public TreeMap<Date, Status> getStatus() {
        return status;
    }

    public TaskMetadata addStatus(Status status) {
        return addStatus(Calendar.getInstance().getTime(), status);
    }

    public TaskMetadata addStatus(Date date, Status status) {
        this.status.put(date, status);
        return this;
    }

    public Type getType() {
        return type;
    }

    public TaskMetadata setType(Type type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("operationName", operationName)
                .append("fileIds", fileIds)
                .append("timestamp", timestamp)
                .append("status", status)
                .append("type", type)
                .toString();
    }
}
