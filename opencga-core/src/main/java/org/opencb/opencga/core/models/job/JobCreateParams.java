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

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.result.JobResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JobCreateParams {

    private String id;
    private String description;

    private String executionId;
    private ToolInfo tool;

    private Enums.Priority priority;

    private String commandLine;

    private Map<String, Object> params;

    private String creationDate;
    private String modificationDate;
    private JobInternal internal;

    private TinyFile outDir;
    private List<TinyFile> input;    // input files to this job
    private List<TinyFile> output;   // output files of this job
    private List<String> tags;

    private JobResult result;

    private TinyFile stdout;
    private TinyFile stderr;

    private Map<String, Object> attributes;

    public JobCreateParams() {
    }

    public JobCreateParams(String id, String description, String executionId, ToolInfo tool, Enums.Priority priority, String commandLine,
                           Map<String, Object> params, String creationDate, String modificationDate, JobInternal internal, TinyFile outDir,
                           List<TinyFile> input, List<TinyFile> output, List<String> tags, JobResult result, TinyFile stdout,
                           TinyFile stderr, Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.executionId = executionId;
        this.tool = tool;
        this.priority = priority;
        this.commandLine = commandLine;
        this.params = params;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.internal = internal;
        this.outDir = outDir;
        this.input = input;
        this.output = output;
        this.tags = tags;
        this.result = result;
        this.stdout = stdout;
        this.stderr = stderr;
        this.attributes = attributes;
    }

    public static JobCreateParams of(Job job) {
        return new JobCreateParams(job.getId(), job.getDescription(), job.getExecutionId(), job.getTool(), job.getPriority(),
                job.getCommandLine(), job.getParams(), job.getCreationDate(), job.getModificationDate(),
                job.getInternal() != null ? new JobInternal(job.getInternal().getStatus()) : null, TinyFile.of(job.getOutDir()),
                job.getInput() != null ? job.getInput().stream().map(TinyFile::of).collect(Collectors.toList()) : Collections.emptyList(),
                job.getOutput() != null ? job.getOutput().stream().map(TinyFile::of).collect(Collectors.toList()) : Collections.emptyList(),
                job.getTags(), job.getResult(), TinyFile.of(job.getStdout()), TinyFile.of(job.getStderr()), job.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", executionId='").append(executionId).append('\'');
        sb.append(", tool=").append(tool);
        sb.append(", priority=").append(priority);
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", params=").append(params);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", internal=").append(internal);
        sb.append(", outDir=").append(outDir);
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", tags=").append(tags);
        sb.append(", result=").append(result);
        sb.append(", stdout=").append(stdout);
        sb.append(", stderr=").append(stderr);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }


    public String getId() {
        return id;
    }

    public JobCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public JobCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getExecutionId() {
        return executionId;
    }

    public JobCreateParams setExecutionId(String executionId) {
        this.executionId = executionId;
        return this;
    }

    public ToolInfo getTool() {
        return tool;
    }

    public JobCreateParams setTool(ToolInfo tool) {
        this.tool = tool;
        return this;
    }

    public Enums.Priority getPriority() {
        return priority;
    }

    public JobCreateParams setPriority(Enums.Priority priority) {
        this.priority = priority;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public JobCreateParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public JobCreateParams setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public JobCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public JobCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public JobInternal getInternal() {
        return internal;
    }

    public JobCreateParams setInternal(JobInternal internal) {
        this.internal = internal;
        return this;
    }

    public TinyFile getOutDir() {
        return outDir;
    }

    public JobCreateParams setOutDir(TinyFile outDir) {
        this.outDir = outDir;
        return this;
    }

    public List<TinyFile> getInput() {
        return input;
    }

    public JobCreateParams setInput(List<TinyFile> input) {
        this.input = input;
        return this;
    }

    public List<TinyFile> getOutput() {
        return output;
    }

    public JobCreateParams setOutput(List<TinyFile> output) {
        this.output = output;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public JobCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public JobResult getResult() {
        return result;
    }

    public JobCreateParams setResult(JobResult result) {
        this.result = result;
        return this;
    }

    public TinyFile getStdout() {
        return stdout;
    }

    public JobCreateParams setStdout(TinyFile stdout) {
        this.stdout = stdout;
        return this;
    }

    public TinyFile getStderr() {
        return stderr;
    }

    public JobCreateParams setStderr(TinyFile stderr) {
        this.stderr = stderr;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public JobCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Job toJob() {
        return new Job(id, null, description, executionId, tool, null, commandLine, params, creationDate, null, priority,
                internal != null ? new org.opencb.opencga.core.models.job.JobInternal(internal.getStatus()) : null,
                outDir != null ? outDir.toFile() : null,
                input != null ? input.stream().map(TinyFile::toFile).collect(Collectors.toList()) : null,
                output != null ? output.stream().map(TinyFile::toFile).collect(Collectors.toList()) : null,
                Collections.emptyList(), tags, result, false, stdout != null ? stdout.toFile() : null,
                stderr != null ? stderr.toFile() : null, 1, null, attributes);
    }

    public static class JobInternal {
        private Enums.ExecutionStatus status;

        public JobInternal() {
        }

        public JobInternal(Enums.ExecutionStatus status) {
            this.status = status;
        }

        public Enums.ExecutionStatus getStatus() {
            return status;
        }

        public JobInternal setStatus(Enums.ExecutionStatus status) {
            this.status = status;
            return this;
        }
    }

    public static class TinyFile {
        private String path;

        public TinyFile() {
        }

        public TinyFile(String path) {
            this.path = path;
        }

        public static TinyFile of(File file) {
            if (file != null && StringUtils.isNotEmpty(file.getPath())) {
                return new TinyFile(file.getPath());
            }
            return null;
        }

        public TinyFile setPath(String path) {
            this.path = path;
            return this;
        }

        public String getPath() {
            return path;
        }

        public File toFile() {
            return new File().setPath(path);
        }
    }

}
