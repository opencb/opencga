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

import org.opencb.opencga.catalog.models.acls.JobAclEntry;
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

    private long id;
    private String name;

    /**
     * Id of the user that created the job.
     */
    private String userId;

    /**
     * Tool to be executed.
     */
    private long toolId;

    @Deprecated
    private String toolName;

    /**
     * Job creation date.
     */
    private String creationDate;
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
    private String commandLine;
    private long visits;
    private JobStatus status;
    private long diskUsage;
    private long outDirId;
    private URI tmpOutDirUri;
    private List<Long> input;    // input files to this job
    private List<Long> output;   // output files of this job
    private List<String> tags;
    private List<JobAclEntry> acl;

    private Map<String, String> params;
    private Map<String, Object> attributes;
    private Map<String, Object> resourceManagerAttributes;
    private String error;
    private String errorDescription;


    public Job() {
    }

    public Job(String name, String userId, String toolName, String description, String commandLine, long outDirId,
               URI tmpOutDirUri, List<Long> input) {
        this(-1, name, userId, -1, toolName, TimeUtils.getTime(), description, System.currentTimeMillis(), -1, "", null, commandLine,
                -1, new JobStatus(JobStatus.PREPARED), 0, outDirId, tmpOutDirUri, input, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(), new HashMap<>(), new HashMap<>(), new HashMap<>(), null);
    }

    @Deprecated
    public Job(long id, String name, String userId, String toolName, String creationDate, String description, long startTime, long endTime,
               String outputError, String commandLine, long visits, JobStatus jobStatus, long diskUsage, long outDirId, URI tmpOutDirUri,
               List<Long> input, List<Long> output, List<String> tags, Map<String, Object> attributes,
               Map<String, Object> resourceManagerAttributes) {
        this.id = id;
        this.name = name;
        this.userId = userId;
        this.toolName = toolName;
        this.creationDate = creationDate;
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
        this.acl = Collections.emptyList();
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

    public Job(long id, String name, String userId, long toolId, String toolName, String creationDate, String description, long startTime,
               long endTime, String outputError, String execution, String commandLine, long visits, JobStatus status, long diskUsage,
               long outDirId, URI tmpOutDirUri, List<Long> input, List<Long> output, List<String> tags, List<JobAclEntry> acl,
               Map<String, String> params, Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes, String error) {
        this.id = id;
        this.name = name;
        this.userId = userId;
        this.toolId = toolId;
        this.toolName = toolName;
        this.creationDate = creationDate;
        this.description = description;
        this.startTime = startTime;
        this.endTime = endTime;
        this.outputError = outputError;
        this.execution = execution;
        this.commandLine = commandLine;
        this.visits = visits;
        this.status = status;
        this.diskUsage = diskUsage;
        this.outDirId = outDirId;
        this.tmpOutDirUri = tmpOutDirUri;
        this.input = input;
        this.output = output;
        this.tags = tags;
        this.acl = acl;
        this.params = params;
        this.attributes = attributes;
        this.resourceManagerAttributes = resourceManagerAttributes;
        if (this.resourceManagerAttributes == null) {
            this.resourceManagerAttributes = new HashMap<>();
        }
        if (!this.resourceManagerAttributes.containsKey(Job.JOB_SCHEDULER_NAME)) {
            this.resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, "");
        }
        if (!this.attributes.containsKey(Job.TYPE)) {
            this.attributes.put(Job.TYPE, Type.ANALYSIS);
        }
        this.error = error;
        this.errorDescription = null;
    }

    public Job(long id, String name, String userId, long toolId, String toolName, String creationDate, String description, long startTime,
               long endTime, String outputError, String execution, String commandLine, long visits, JobStatus status, long diskUsage,
               long outDirId, URI tmpOutDirUri, List<Long> input, List<Long> output, List<String> tags, List<JobAclEntry> acl,
               Map<String, String> params, Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes, String error,
               String errorDescription) {
        this.id = id;
        this.name = name;
        this.userId = userId;
        this.toolId = toolId;
        this.toolName = toolName;
        this.creationDate = creationDate;
        this.description = description;
        this.startTime = startTime;
        this.endTime = endTime;
        this.outputError = outputError;
        this.execution = execution;
        this.commandLine = commandLine;
        this.visits = visits;
        this.status = status;
        this.diskUsage = diskUsage;
        this.outDirId = outDirId;
        this.tmpOutDirUri = tmpOutDirUri;
        this.input = input;
        this.output = output;
        this.tags = tags;
        this.acl = acl;
        this.params = params;
        this.attributes = attributes;
        this.resourceManagerAttributes = resourceManagerAttributes;
        if (this.resourceManagerAttributes == null) {
            this.resourceManagerAttributes = new HashMap<>();
        }
        if (!this.resourceManagerAttributes.containsKey(Job.JOB_SCHEDULER_NAME)) {
            this.resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, "");
        }
        if (!this.attributes.containsKey(Job.TYPE)) {
            this.attributes.put(Job.TYPE, Type.ANALYSIS);
        }
        this.error = error;
        this.errorDescription = errorDescription;
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


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Job{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", toolId=").append(toolId);
        sb.append(", toolName='").append(toolName).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", startTime=").append(startTime);
        sb.append(", endTime=").append(endTime);
        sb.append(", outputError='").append(outputError).append('\'');
        sb.append(", execution='").append(execution).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", visits=").append(visits);
        sb.append(", status=").append(status);
        sb.append(", diskUsage=").append(diskUsage);
        sb.append(", outDirId=").append(outDirId);
        sb.append(", tmpOutDirUri=").append(tmpOutDirUri);
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", tags=").append(tags);
        sb.append(", acl=").append(acl);
        sb.append(", params=").append(params);
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

    public Job setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Job setName(String name) {
        this.name = name;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public Job setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public long getToolId() {
        return toolId;
    }

    public Job setToolId(long toolId) {
        this.toolId = toolId;
        return this;
    }

    public String getToolName() {
        return toolName;
    }

    public Job setToolName(String toolName) {
        this.toolName = toolName;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Job setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Job setDescription(String description) {
        this.description = description;
        return this;
    }

    public long getStartTime() {
        return startTime;
    }

    public Job setStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public long getEndTime() {
        return endTime;
    }

    public Job setEndTime(long endTime) {
        this.endTime = endTime;
        return this;
    }

    public String getOutputError() {
        return outputError;
    }

    public Job setOutputError(String outputError) {
        this.outputError = outputError;
        return this;
    }

    public String getExecution() {
        return execution;
    }

    public Job setExecution(String execution) {
        this.execution = execution;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public Job setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public long getVisits() {
        return visits;
    }

    public Job setVisits(long visits) {
        this.visits = visits;
        return this;
    }

    public JobStatus getStatus() {
        return status;
    }

    public Job setStatus(JobStatus status) {
        this.status = status;
        return this;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public Job setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
        return this;
    }

    public long getOutDirId() {
        return outDirId;
    }

    public Job setOutDirId(long outDirId) {
        this.outDirId = outDirId;
        return this;
    }

    public URI getTmpOutDirUri() {
        return tmpOutDirUri;
    }

    public Job setTmpOutDirUri(URI tmpOutDirUri) {
        this.tmpOutDirUri = tmpOutDirUri;
        return this;
    }

    public List<Long> getInput() {
        return input;
    }

    public Job setInput(List<Long> input) {
        this.input = input;
        return this;
    }

    public List<Long> getOutput() {
        return output;
    }

    public Job setOutput(List<Long> output) {
        this.output = output;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public Job setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public List<JobAclEntry> getAcl() {
        return acl;
    }

    public Job setAcl(List<JobAclEntry> acl) {
        this.acl = acl;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Job setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Job setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Map<String, Object> getResourceManagerAttributes() {
        return resourceManagerAttributes;
    }

    public Job setResourceManagerAttributes(Map<String, Object> resourceManagerAttributes) {
        this.resourceManagerAttributes = resourceManagerAttributes;
        return this;
    }

    public String getError() {
        return error;
    }

    public Job setError(String error) {
        this.error = error;
        return this;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    public Job setErrorDescription(String errorDescription) {
        this.errorDescription = errorDescription;
        return this;
    }

}
