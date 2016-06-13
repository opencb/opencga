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

import org.opencb.opencga.catalog.models.acls.JobAcl;
import org.opencb.opencga.core.common.TimeUtils;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Job {

    /* Attributes known keys */
    public static final String TYPE = "type";
    public static final String INDEXED_FILE_ID = "indexedFileId";
    /* ResourceManagerAttributes known keys */
    public static final String JOB_SCHEDULER_NAME = "jobSchedulerName";
    /* Errors */
    public static final Map<String, String> ERROR_DESCRIPTIONS;
    public static final String ERRNO_NONE = null;
    public static final String ERRNO_NO_QUEUE = "ERRNO_NO_QUEUE";
    public static final String ERRNO_FINISH_ERROR = "ERRNO_FINISH_ERROR";
    public static final String ERRNO_ABORTED = "ERRNO_ABORTED";

    static {
        ERROR_DESCRIPTIONS = new HashMap<>();
        ERROR_DESCRIPTIONS.put(ERRNO_NONE, null);
        ERROR_DESCRIPTIONS.put(ERRNO_NO_QUEUE, "Unable to queue job");
        ERROR_DESCRIPTIONS.put(ERRNO_FINISH_ERROR, "Job finished with exit value != 0");
        ERROR_DESCRIPTIONS.put(ERRNO_ABORTED, "Job aborted");
    }

    /**
     * Catalog unique identifier.
     */
    private long id;

    /**
     * User given job name.
     */
    private String name;

    /**
     * UserId of the user that created the job.
     */
    private String userId;

    /**
     * Name of the tool to be executed.
     */
    private String toolName;

    /**
     * Job creation date.
     */
    private String date;
    private String description;

    /**
     * Start time in milliseconds.
     */
    private long startTime;

    /**
     * End time in milliseconds.
     */
    private long endTime;
    private String outputError;
    private String execution;
    private Map<String, String> params;
    private String commandLine;
    private long visits;
    private JobStatus status;
    private long diskUsage;
    private long outDirId;
    private URI tmpOutDirUri;
    private List<Long> input;    // input files to this job
    private List<Long> output;   // output files of this job
    private List<String> tags;
    private List<JobAcl> acls;
    private Map<String, Object> attributes;
    private Map<String, Object> resourceManagerAttributes;
    private String error;
    private String errorDescription;

    public Job() {
    }

    public Job(String name, String userId, String toolName, String description, String commandLine, long outDirId, URI tmpOutDirUri,
               List<Long> input) {
        this(-1, name, userId, toolName, TimeUtils.getTime(), description, System.currentTimeMillis(), -1, "", commandLine, -1,
                new JobStatus(JobStatus.PREPARED), 0, outDirId, tmpOutDirUri, input, new LinkedList<>(), new LinkedList<>(),
                new HashMap<>(), new HashMap<>());
    }

    public Job(long id, String name, String userId, String toolName, String date, String description, long startTime, long endTime,
               String outputError, String commandLine, long visits, JobStatus jobStatus, long diskUsage, long outDirId, URI tmpOutDirUri,
               List<Long> input, List<Long> output, List<String> tags, Map<String, Object> attributes,
               Map<String, Object> resourceManagerAttributes) {
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
        this.status = jobStatus;
        this.diskUsage = diskUsage;
        this.outDirId = outDirId;
//        this.tmpOutDirId = tmpOutDirId;
        this.tmpOutDirUri = tmpOutDirUri;
//        this.outDir = outDir;
        this.input = input;
        this.output = output;
        this.acls = Collections.emptyList();
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
        final StringBuilder sb = new StringBuilder("Job{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", toolName='").append(toolName).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", startTime=").append(startTime);
        sb.append(", endTime=").append(endTime);
        sb.append(", outputError='").append(outputError).append('\'');
        sb.append(", execution='").append(execution).append('\'');
        sb.append(", params=").append(params);
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", visits=").append(visits);
        sb.append(", status=").append(status);
        sb.append(", diskUsage=").append(diskUsage);
        sb.append(", outDirId=").append(outDirId);
        sb.append(", tmpOutDirUri=").append(tmpOutDirUri);
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", tags=").append(tags);
        sb.append(", acls=").append(acls);
        sb.append(", attributes=").append(attributes);
        sb.append(", resourceManagerAttributes=").append(resourceManagerAttributes);
        sb.append(", error='").append(error).append('\'');
        sb.append(", errorDescription='").append(errorDescription).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
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

    public long getVisits() {
        return visits;
    }

    public void setVisits(long visits) {
        this.visits = visits;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
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

    public long getOutDirId() {
        return outDirId;
    }

    public void setOutDirId(long outDirId) {
        this.outDirId = outDirId;
    }

    public URI getTmpOutDirUri() {
        return tmpOutDirUri;
    }

    public void setTmpOutDirUri(URI tmpOutDirUri) {
        this.tmpOutDirUri = tmpOutDirUri;
    }

    public List<Long> getInput() {
        return input;
    }

    public void setInput(List<Long> input) {
        this.input = input;
    }

    public List<Long> getOutput() {
        return output;
    }

    public void setOutput(List<Long> output) {
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

    public List<JobAcl> getAcls() {
        return acls;
    }

    public Job setAcls(List<JobAcl> acls) {
        this.acls = acls;
        return this;
    }

    public static class JobStatus extends Status {

        /**
         * PREPARED status means that the job is ready to be put into the queue.
         */
        public static final String PREPARED = "PREPARED";
        /**
         * QUEUED status means that the job is waiting on the queue to have an available slot for execution.
         */
        public static final String QUEUED = "QUEUED";
        /**
         * RUNNING status means that the job is running.
         */
        public static final String RUNNING = "RUNNING";
        /**
         * DONE status means that the job has finished the execution, but the output is still not ready.
         */
        public static final String DONE = "DONE";
        /**
         * ERROR status means that the job finished with an error.
         */
        public static final String ERROR = "ERROR";

        public JobStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public JobStatus(String status) {
            this(status, "");
        }

        public JobStatus() {
            this(PREPARED, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (status != null && (status.equals(PREPARED) || status.equals(QUEUED) || status.equals(RUNNING) || status.equals(DONE)
                    || status.equals(ERROR))) {
                return true;
            }
            return false;
        }

    }

    public enum Type {
        ANALYSIS,
        INDEX,
        COHORT_STATS
    }
}
