/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.models.job;

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.result.JobResult;

import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
@DataClass(id = "Job", since = "1.0",
        description = "Job data model hosts information about any job.")
public class Job extends PrivateStudyUid {

    public static final String OPENCGA_PARENTS = "OPENCGA_PARENTS";
    /**
     * Job ID is a mandatory parameter when creating a new sample, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */
    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;
    /**
     * Global unique ID at the whole OpenCGA installation. This is automatically created during the sample creation and cannot be changed.
     *
     * @apiNote Internal, Unique, Immutable
     */

    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;
    /**
     * An string to describe the properties of the Job.
     *
     * @apiNote
     */
    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "executionId", description = FieldConstants.JOB_EXECUTION_ID)
    private String executionId;

    @DataField(id = "tool", indexed = true,
            description = FieldConstants.JOB_TOOL)
    private ToolInfo tool;

    @DataField(id = "userId", indexed = true,
            description = FieldConstants.JOB_USER_ID)
    private String userId;

    @DataField(id = "commandLine", indexed = true,
            description = FieldConstants.JOB_COMMAND_LINE)
    private String commandLine;


    @DataField(id = "params", indexed = true,
            description = FieldConstants.JOB_PARAMS)
    private Map<String, Object> params;

    @DataField(id = "creationDate", indexed = true,
            description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, since = "1.0",
            description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "priority", indexed = true,
            description = FieldConstants.JOB_PRIORITY_DESCRIPTION)
    private Enums.Priority priority;

    @DataField(id = "release", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private JobInternal internal;

    @DataField(id = "outDir", indexed = true,
            description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private File outDir;

    @DataField(id = "input", indexed = true,
            description = FieldConstants.JOB_INPUT_DESCRIPTION)
    private List<File> input;    // input files to this job

    @DataField(id = "output", indexed = true,
            description = FieldConstants.JOB_OUTPUT_DESCRIPTION)
    private List<File> output;   // output files of this job

    @DataField(id = "tags", indexed = true,
            description = FieldConstants.JOB_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "dependsOn", indexed = true,
            description = FieldConstants.JOB_DEPENDS_ON_DESCRIPTION)
    private List<Job> dependsOn;

    @DataField(id = "execution", indexed = true, description = FieldConstants.JOB_EXECUTION_DESCRIPTION)
    private JobResult execution;

    @DataField(id = "stdout", indexed = true,
            description = FieldConstants.JOB_STDOUT_DESCRIPTION)
    private File stdout;

    @DataField(id = "stderr", indexed = true,
            description = FieldConstants.JOB_STDERR_DESCRIPTION)
    private File stderr;

    @DataField(id = "visited", indexed = true,
            description = FieldConstants.JOB_VISITED)
    private boolean visited;
    /**
     * An integer describing the current data release.
     *
     * @apiNote Internal
     */

    @DataField(id = "release", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;

    @DataField(id = "study", indexed = true,
            description = FieldConstants.JOB_STUDY)
    private JobStudyParam study;

    @DataField(id = "attributes", indexed = true,
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public Job() {
    }

    public Job(String id, String uuid, String description, String executionId, ToolInfo tool, String userId, String commandLine,
               Map<String, Object> params, String creationDate, String modificationDate, Enums.Priority priority, JobInternal internal,
               File outDir, List<File> input, List<File> output, List<Job> dependsOn, List<String> tags, JobResult execution,
               boolean visited, File stdout, File stderr, int release, JobStudyParam study, Map<String, Object> attributes) {
        this.id = id;
        this.uuid = uuid;
        this.tool = tool;
        this.description = description;
        this.executionId = executionId;
        this.userId = userId;
        this.commandLine = commandLine;
        this.params = params;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.priority = priority;
        this.internal = internal != null ? internal : new JobInternal();
        this.outDir = outDir;
        this.input = input;
        this.output = output;
        this.dependsOn = dependsOn;
        this.tags = tags;
        this.execution = execution;
        this.visited = visited;
        this.stdout = stdout;
        this.stderr = stderr;
        this.release = release;
        this.study = study;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Job{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", executionId='").append(executionId).append('\'');
        sb.append(", tool=").append(tool);
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", params=").append(params);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", priority=").append(priority);
        sb.append(", internal=").append(internal);
        sb.append(", outDir=").append(outDir);
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", tags=").append(tags);
        sb.append(", dependsOn=").append(dependsOn);
        sb.append(", execution=").append(execution);
        sb.append(", stdout=").append(stdout);
        sb.append(", stderr=").append(stderr);
        sb.append(", visited=").append(visited);
        sb.append(", release=").append(release);
        sb.append(", study=").append(study);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public Job setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    @Override
    public Job setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Job setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public Job setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Job setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getExecutionId() {
        return executionId;
    }

    public Job setExecutionId(String executionId) {
        this.executionId = executionId;
        return this;
    }

    public ToolInfo getTool() {
        return tool;
    }

    public Job setTool(ToolInfo tool) {
        this.tool = tool;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public Job setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public Job setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public Job setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Job setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Job setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Enums.Priority getPriority() {
        return priority;
    }

    public Job setPriority(Enums.Priority priority) {
        this.priority = priority;
        return this;
    }

    public JobInternal getInternal() {
        return internal;
    }

    public Job setInternal(JobInternal internal) {
        this.internal = internal;
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

    public List<Job> getDependsOn() {
        return dependsOn;
    }

    public Job setDependsOn(List<Job> dependsOn) {
        this.dependsOn = dependsOn;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public Job setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public boolean isVisited() {
        return visited;
    }

    public Job setVisited(boolean visited) {
        this.visited = visited;
        return this;
    }

    public JobResult getExecution() {
        return execution;
    }

    public Job setExecution(JobResult execution) {
        this.execution = execution;
        return this;
    }

    public File getStdout() {
        return stdout;
    }

    public Job setStdout(File stdout) {
        this.stdout = stdout;
        return this;
    }

    public File getStderr() {
        return stderr;
    }

    public Job setStderr(File stderr) {
        this.stderr = stderr;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Job setRelease(int release) {
        this.release = release;
        return this;
    }

    public JobStudyParam getStudy() {
        return study;
    }

    public Job setStudy(JobStudyParam study) {
        this.study = study;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Job setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
