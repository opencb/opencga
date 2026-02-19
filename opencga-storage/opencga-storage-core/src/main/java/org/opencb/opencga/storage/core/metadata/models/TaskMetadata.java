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
        /**
         * Active task.
         * Running, but not finished
         */
        RUNNING,
        /**
         * Active task.
         * Finished, but some work still needed (optional status)
         */
        DONE,
        /**
         * Active task.
         * Currently, paused.
         * Errors found during the execution. Needs to be resumed or cleaned.
         */
        ERROR,
        /**
         * Finished.
         * Ready to be used
         */
        READY,
        /**
         * Finished.
         * Task was aborted, cancelled or rolled back.
         * Any needed clean might be executed by other running tasks
         */
        ABORTED,
        /**
         * Finished.
         * Task finished with invalid results.
         * Similar to "ERROR" status, but this can't be resumed. Needs to be cleaned first.
         */
        INVALID,
    }

    public enum Type {
        LOAD,
        REMOVE,
        OTHER
    }

    private int studyId;
    private int id;
    private String name;
    private List<Integer> fileIds;
    private long timestamp;
    private final TreeMap<Date, Status> status = new TreeMap<>(Date::compareTo);
    private Type type = Type.OTHER;

    public TaskMetadata() {
    }

    public TaskMetadata(int studyId, int id, String name, List<Integer> fileIds, long timestamp, Type type) {
        this.studyId = studyId;
        this.id = id;
        this.name = name;
        this.fileIds = fileIds;
        this.timestamp = timestamp;
        this.type = type;
    }

    public TaskMetadata(TaskMetadata batch) {
        this();
        this.studyId = batch.studyId;
        this.id = batch.id;
        this.name = batch.name;
        this.fileIds = new ArrayList<>(batch.fileIds);
        this.timestamp = batch.timestamp;
        this.status.putAll(batch.status);
        this.type = batch.type;
    }

    public boolean sameOperation(Collection<Integer> fileIds, Type type, String jobOperationName) {
        return this.type.equals(type)
                && this.name.equals(jobOperationName)
                && fileIds.size() == this.fileIds.size()
                && fileIds.containsAll(this.fileIds);
    }

    public Status currentStatus() {
        return status.isEmpty() ? null : status.lastEntry().getValue();
    }

    public String getName() {
        return name;
    }

    public TaskMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public String getOperationName() {
        return getName();
    }

    public TaskMetadata setOperationName(String name) {
        return setName(name);
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
                .append("studyId", studyId)
                .append("operationName", name)
                .append("fileIds", fileIds)
                .append("timestamp", timestamp)
                .append("status", status)
                .append("type", type)
                .toString();
    }
}
