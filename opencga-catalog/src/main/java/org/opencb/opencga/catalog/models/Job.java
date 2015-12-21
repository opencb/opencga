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

import org.opencb.opencga.core.common.TimeUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Job {

    /**
     * Catalog unique identifier
     */
    private int id;
    /**
     * User given job name
     */
    private String name;
    /**
     * UserId of the user that created the job
     */
    private String userId;
    /**
     * Name of the tool to be executed
     */
    private String toolName;
    /**
     * Job creation date
     */
    private String date;

    private String description;
    /**
     * Start time in milliseconds
     */
    private long startTime;
    /**
     * End time in milliseconds
     */
    private long endTime;
    private String outputError;
    private String execution;
    private Map<String, String> params;
    private String commandLine;
    private int visits;
    private Status status;
    private long diskUsage;

    private int outDirId;
    private URI tmpOutDirUri;

    private List<Integer> input;    // input files to this job
    private List<Integer> output;   // output files of this job

    private List<String> tags;
    private Map<String, Object> attributes;

    private Map<String, Object> resourceManagerAttributes;

    private String error;
    private String errorDescription;

    public enum Status {
       PREPARED,              //Job is ready to be executed. Daemon will enqueue it.
       ERROR,                 //Job with errors. See "error"
       QUEUED,
       RUNNING,
       DONE,                  //Job finished, but output not ready. Daemon will process the output.
//       PROCESSING_OUTPUT,     //Job finished, but output not ready. Daemon will process the output.
       READY,                 //Job finished and ready
    }

    public enum Type {
        ANALYSIS,
        INDEX,
        COHORT_STATS,
    }

    /* Attributes known keys */
    public static final String TYPE = "type";
    public static final String INDEXED_FILE_ID = "indexedFileId";

    /* ResourceManagerAttributes known keys */
    public static final String JOB_SCHEDULER_NAME = "jobSchedulerName";

    /* Errors */
    public static final Map<String, String> errorDescriptions;

    public static final String ERRNO_NONE = null;
    public static final String ERRNO_NO_QUEUE = "ERRNO_NO_QUEUE";
    public static final String ERRNO_FINISH_ERROR = "ERRNO_FINISH_ERROR";
    public static final String ERRNO_ABORTED = "ERRNO_ABORTED";

    static {
        errorDescriptions = new HashMap<>();
        errorDescriptions.put(ERRNO_NONE, null);
        errorDescriptions.put(ERRNO_NO_QUEUE, "Unable to queue job");
        errorDescriptions.put(ERRNO_FINISH_ERROR, "Job finished with exit value != 0");
        errorDescriptions.put(ERRNO_ABORTED, "Job aborted");

    }

    public Job() {
    }


    public Job(String name, String userId, String toolName, String description, String commandLine, int outDirId,
               URI tmpOutDirUri, List<Integer> input) {
        this(-1, name, userId, toolName, TimeUtils.getTime(), description, System.currentTimeMillis(), -1, "", commandLine, -1, Status.PREPARED, 0,
                outDirId, tmpOutDirUri, input, new LinkedList<>(), new LinkedList<>(), new HashMap<>(),
                new HashMap<>());
    }

    public Job(int id, String name, String userId, String toolName, String date, String description,
               long startTime, long endTime, String outputError, String commandLine, int visits, Status status,
               long diskUsage, int outDirId, URI tmpOutDirUri, List<Integer> input,
               List<Integer> output, List<String> tags, Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes) {
        this.id = id;
        this.name = name;
        this.userId = userId;
        this.toolName = toolName;
        this.date = date;
        this.description = description;
        this.startTime = startTime;
        this.endTime = endTime;
        this.outputError = outputError;
        this.commandLine = commandLine;
        this.visits = visits;
        this.status = status;
        this.diskUsage = diskUsage;
        this.outDirId = outDirId;
//        this.tmpOutDirId = tmpOutDirId;
        this.tmpOutDirUri = tmpOutDirUri;
//        this.outDir = outDir;
        this.input = input;
        this.output = output;
        this.tags = tags;
        this.attributes = attributes;
        this.resourceManagerAttributes = resourceManagerAttributes;
        if (this.resourceManagerAttributes == null) {
            this.resourceManagerAttributes = new HashMap<>();
        }
        //Initializing attributes maps.
        if (!this.resourceManagerAttributes.containsKey(Job.JOB_SCHEDULER_NAME)) {
            this.resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, "");
        }
        if (!this.attributes.containsKey(Job.TYPE)) {
            this.attributes.put(Job.TYPE, Type.ANALYSIS);
        }
        error = ERRNO_NONE;
        errorDescription = null;
    }

    @Override
    public String toString() {
        return "Job{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", userId='" + userId + '\'' +
                ", toolName='" + toolName + '\'' +
                ", date='" + date + '\'' +
                ", description='" + description + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", outputError='" + outputError + '\'' +
                ", commandLine='" + commandLine + '\'' +
                ", visits=" + visits +
                ", status=" + status +
                ", diskUsage=" + diskUsage +
                ", execution='" + execution + '\'' +
                ", params=" + params +
                ", outDirId=" + outDirId +
                ", tmpOutDirUri=" + tmpOutDirUri +
                ", input=" + input +
                ", output=" + output +
                ", tags=" + tags +
                ", attributes=" + attributes +
                ", resourceManagerAttributes=" + resourceManagerAttributes +
                ", error='" + error + '\'' +
                ", errorDescription='" + errorDescription + '\'' +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getToolName() {
        return toolName;
    }

    public void setToolName(String toolName) {
        this.toolName = toolName;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getOutputError() {
        return outputError;
    }

    public void setOutputError(String outputError) {
        this.outputError = outputError;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public void setCommandLine(String commandLine) {
        this.commandLine = commandLine;
    }

    public int getVisits() {
        return visits;
    }

    public void setVisits(int visits) {
        this.visits = visits;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public String getExecution() {
        return execution;
    }

    public Job setExecution(String execution) {
        this.execution = execution;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Job setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

    public int getOutDirId() {
        return outDirId;
    }

    public void setOutDirId(int outDirId) {
        this.outDirId = outDirId;
    }

    public URI getTmpOutDirUri() {
        return tmpOutDirUri;
    }

    public void setTmpOutDirUri(URI tmpOutDirUri) {
        this.tmpOutDirUri = tmpOutDirUri;
    }

    public List<Integer> getInput() {
        return input;
    }

    public void setInput(List<Integer> input) {
        this.input = input;
    }

    public List<Integer> getOutput() {
        return output;
    }

    public void setOutput(List<Integer> output) {
        this.output = output;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }


    public Map<String, Object> getResourceManagerAttributes() {
        return resourceManagerAttributes;
    }

    public void setResourceManagerAttributes(Map<String, Object> resourceManagerAttributes) {
        this.resourceManagerAttributes = resourceManagerAttributes;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    public void setErrorDescription(String errorDescription) {
        this.errorDescription = errorDescription;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
