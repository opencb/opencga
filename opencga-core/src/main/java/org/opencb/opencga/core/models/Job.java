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

package org.opencb.opencga.core.models;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.*;

import static org.opencb.opencga.core.common.FieldUtils.defaultObject;

/**
 * Created by jacobo on 11/09/14.
 */
public class Job {

    /* Attributes known keys */
    @Deprecated
    public static final String TYPE = "type";
    public static final String INDEXED_FILE_ID = "indexedFileId";

    public static final String OPENCGA_OUTPUT_DIR = "OPENCGA_OUTPUT_DIR";
    public static final String OPENCGA_TMP_DIR = "OPENCGA_TMP_DIR";
    public static final String OPENCGA_STUDY = "OPENCGA_STUDY";

    /* ResourceManagerAttributes known keys */
    public static final String JOB_SCHEDULER_NAME = "jobSchedulerName";
    /* Errors */
    public static final Map<String, String> ERROR_DESCRIPTIONS;
    public static final String ERRNO_NONE = null;
    public static final String ERRNO_NO_QUEUE = "ERRNO_NO_QUEUE";
    public static final String ERRNO_FINISH_ERROR = "ERRNO_FINISH_ERROR";
    public static final String ERRNO_ABORTED = "ERRNO_ABORTED";

    static {
        HashMap<String, String> map = new HashMap<>();
        map.put(ERRNO_NONE, null);
        map.put(ERRNO_NO_QUEUE, "Unable to queue job");
        map.put(ERRNO_FINISH_ERROR, "Job finished with exit value != 0");
        map.put(ERRNO_ABORTED, "Job aborted");
        ERROR_DESCRIPTIONS = Collections.unmodifiableMap(map);
    }

    private long id;
    private String name;

    /**
     * Id of the user that created the job.
     */
    private String userId;

    private String toolId;

    private Type type;
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
    private String execution;
    @Deprecated
    private String executable;
    private String commandLine;
    private boolean visited;
    private JobStatus status;
    private long size;
    private File outDir;
    private List<File> input;    // input files to this job
    private List<File> output;   // output files of this job
    private List<String> tags;

    private File stdOutput;
    private File stdError;

    private Map<String, String> params;
    private Map<String, Object> resourceManagerAttributes;

    private int release;
    private Map<String, Object> attributes;

    public Job() {
    }

    public Job(String name, String toolId, String execution, Type type, String description, Map<String, String> params,
               Map<String, Object> attributes) {
        this(-1, name, "", toolId, type, TimeUtils.getTime(), description, -1, -1, execution, "", "", false, new JobStatus(), -1, null,
                null, null, null, params, -1, attributes, null);
    }

    public Job(String name, String userId, String executable, Type type, List<File> input, List<File> output, File outDir,
               Map<String, String> params, int release) {
        this(-1, name, userId, "", type, TimeUtils.getTime(), "", -1, -1, "", executable, "", false, new JobStatus(JobStatus.PREPARED),
                -1, outDir, input, output, null, params, release, null, null);
    }

    public Job(String name, String userId, String toolName, String description, String commandLine, File outDir, List<File> input,
               int release) {
        // FIXME: Modify this to take into account both toolName and executable for RC2
        this(-1, name, userId, toolName, Type.ANALYSIS, TimeUtils.getTime(), description, System.currentTimeMillis(), -1, null,
                null, commandLine, false, new JobStatus(JobStatus.PREPARED), 0, outDir, input, null, null, null, release, null, null);
    }

    public Job(long id, String name, String userId, String toolId, Type type, String creationDate, String description, long startTime,
               long endTime, String execution, String executable, String commandLine, boolean visited, JobStatus status, long size,
               File outDir, List<File> input, List<File> output, List<String> tags, Map<String, String> params, int release,
               Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes) {
        this.id = id;
        this.name = name;
        this.userId = userId;
        this.toolId = toolId;
        this.type = type;
        this.creationDate = creationDate;
        this.description = description;
        this.startTime = startTime;
        this.endTime = endTime;
        this.execution = execution != null ? execution : "";
        this.executable = executable != null ? executable : "";
        this.commandLine = commandLine;
        this.visited = visited;
        this.status = status;
        this.size = size;
        this.outDir = outDir;
        this.input = defaultObject(input, ArrayList::new);
        this.output = defaultObject(output, ArrayList::new);
        this.tags = defaultObject(tags, ArrayList::new);
        this.params = params;
        this.release = release;
        this.attributes = defaultObject(attributes, HashMap::new);
        this.resourceManagerAttributes = defaultObject(resourceManagerAttributes, HashMap::new);
        this.params = defaultObject(params, HashMap::new);
        this.release = release;
        this.resourceManagerAttributes.putIfAbsent(Job.JOB_SCHEDULER_NAME, "");
        this.attributes.putIfAbsent(Job.TYPE, Type.ANALYSIS);
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
        /**
         * UNKNOWN status means that the job status could not be obtained.
         */
        public static final String UNKNOWN = "UNKNOWN";

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
                    || status.equals(ERROR) || status.equals(UNKNOWN))) {
                return true;
            }
            return false;
        }

    }

    public enum Type {
        INDEX,
        ANALYSIS
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Job{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", toolId='").append(toolId).append('\'');
        sb.append(", type=").append(type);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", startTime=").append(startTime);
        sb.append(", endTime=").append(endTime);
        sb.append(", execution='").append(execution).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", visited=").append(visited);
        sb.append(", status=").append(status);
        sb.append(", size=").append(size);
        sb.append(", outDir=").append(outDir);
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", tags=").append(tags);
        sb.append(", stdOutput=").append(stdOutput);
        sb.append(", stdError=").append(stdError);
        sb.append(", params=").append(params);
        sb.append(", resourceManagerAttributes=").append(resourceManagerAttributes);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
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

    public Type getType() {
        return type;
    }

    public Job setType(Type type) {
        this.type = type;
        return this;
    }

    public String getExecutable() {
        return executable;
    }

    public Job setExecutable(String executable) {
        this.executable = executable;
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

    public boolean isVisited() {
        return visited;
    }

    public Job setVisited(boolean visited) {
        this.visited = visited;
        return this;
    }

    public JobStatus getStatus() {
        return status;
    }

    public Job setStatus(JobStatus status) {
        this.status = status;
        return this;
    }

    public long getSize() {
        return size;
    }

    public Job setSize(long size) {
        this.size = size;
        return this;
    }

    public File getOutDir() {
        return outDir;
    }

    public Job setOutDir(File outDir) {
        this.outDir = outDir;
        return this;
    }

    public List<File> getInput() {
        return input;
    }

    public Job setInput(List<File> input) {
        this.input = input;
        return this;
    }

    public List<File> getOutput() {
        return output;
    }

    public Job setOutput(List<File> output) {
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

    public Map<String, String> getParams() {
        return params;
    }

    public Job setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

    public File getStdOutput() {
        return stdOutput;
    }

    public Job setStdOutput(File stdOutput) {
        this.stdOutput = stdOutput;
        return this;
    }

    public File getStdError() {
        return stdError;
    }

    public Job setStdError(File stdError) {
        this.stdError = stdError;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Job setRelease(int release) {
        this.release = release;
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

    public String getToolId() {
        return toolId;
    }

    public Job setToolId(String toolId) {
        this.toolId = toolId;
        return this;
    }

}
