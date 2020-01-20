package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JobCreateParams {

    private String id;
    private String description;

    private ToolInfo tool;

    private Enums.Priority priority;

    private String commandLine;

    private Map<String, Object> params;

    private String creationDate;
    private Enums.ExecutionStatus status;

    private TinyFile outDir;
    private List<TinyFile> input;    // input files to this job
    private List<TinyFile> output;   // output files of this job
    private List<String> tags;

    private ExecutionResult result;

    private TinyFile log;
    private TinyFile errorLog;

    private Map<String, Object> attributes;

    public JobCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", tool=").append(tool);
        sb.append(", priority=").append(priority);
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", params=").append(params);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", outDir=").append(outDir);
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", tags=").append(tags);
        sb.append(", result=").append(result);
        sb.append(", log=").append(log);
        sb.append(", errorLog=").append(errorLog);
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

    public Enums.ExecutionStatus getStatus() {
        return status;
    }

    public JobCreateParams setStatus(Enums.ExecutionStatus status) {
        this.status = status;
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

    public ExecutionResult getResult() {
        return result;
    }

    public JobCreateParams setResult(ExecutionResult result) {
        this.result = result;
        return this;
    }

    public TinyFile getLog() {
        return log;
    }

    public JobCreateParams setLog(TinyFile log) {
        this.log = log;
        return this;
    }

    public TinyFile getErrorLog() {
        return errorLog;
    }

    public JobCreateParams setErrorLog(TinyFile errorLog) {
        this.errorLog = errorLog;
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
        return new Job(id, null, description, tool, null, commandLine, params, creationDate, null, priority, status,
                outDir != null ? outDir.toFile() : null,
                getInput().stream().map(TinyFile::toFile).collect(Collectors.toList()),
                getOutput().stream().map(TinyFile::toFile).collect(Collectors.toList()), Collections.emptyList(),
                tags, result, false, log != null ? log.toFile() : null, errorLog != null ? errorLog.toFile() : null, 1, null, attributes);
    }

    public static class TinyFile {
        private String path;

        public TinyFile() {
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
