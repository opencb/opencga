package org.opencb.opencga.master.monitor.models;

import org.opencb.opencga.catalog.models.update.JobUpdateParams;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import java.util.List;
import java.util.Map;

public class PrivateJobUpdateParams extends JobUpdateParams {

    private String commandLine;

    private Map<String, String> params;
    private Enums.ExecutionStatus status;

    private File outDir;
    private File tmpDir;
    private List<File> input;    // input files to this job
    private List<File> output;   // output files of this job

    private ExecutionResult result;

    private File log;
    private File errorLog;

    public PrivateJobUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PrivateJobUpdateParams{");
        sb.append("commandLine='").append(commandLine).append('\'');
        sb.append(", params=").append(params);
        sb.append(", status=").append(status);
        sb.append(", outDir=").append(outDir);
        sb.append(", tmpDir=").append(tmpDir);
        sb.append(", input=").append(input);
        sb.append(", output=").append(output);
        sb.append(", result=").append(result);
        sb.append(", log=").append(log);
        sb.append(", errorLog=").append(errorLog);
        sb.append(", name='").append(getName()).append('\'');
        sb.append(", description='").append(getDescription()).append('\'');
        sb.append(", tags=").append(getTags());
        sb.append(", visited=").append(getVisited());
        sb.append(", attributes=").append(getAttributes());
        sb.append('}');
        return sb.toString();
    }

    public String getCommandLine() {
        return commandLine;
    }

    public PrivateJobUpdateParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public PrivateJobUpdateParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

    public Enums.ExecutionStatus getStatus() {
        return status;
    }

    public PrivateJobUpdateParams setStatus(Enums.ExecutionStatus status) {
        this.status = status;
        return this;
    }

    public File getOutDir() {
        return outDir;
    }

    public PrivateJobUpdateParams setOutDir(File outDir) {
        this.outDir = outDir;
        return this;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public PrivateJobUpdateParams setTmpDir(File tmpDir) {
        this.tmpDir = tmpDir;
        return this;
    }

    public List<File> getInput() {
        return input;
    }

    public PrivateJobUpdateParams setInput(List<File> input) {
        this.input = input;
        return this;
    }

    public List<File> getOutput() {
        return output;
    }

    public PrivateJobUpdateParams setOutput(List<File> output) {
        this.output = output;
        return this;
    }

    public ExecutionResult getResult() {
        return result;
    }

    public PrivateJobUpdateParams setResult(ExecutionResult result) {
        this.result = result;
        return this;
    }

    public File getLog() {
        return log;
    }

    public PrivateJobUpdateParams setLog(File log) {
        this.log = log;
        return this;
    }

    public File getErrorLog() {
        return errorLog;
    }

    public PrivateJobUpdateParams setErrorLog(File errorLog) {
        this.errorLog = errorLog;
        return this;
    }

    @Override
    public PrivateJobUpdateParams setName(String name) {
        super.setName(name);
        return this;
    }

    @Override
    public PrivateJobUpdateParams setDescription(String description) {
        super.setDescription(description);
        return this;
    }

    @Override
    public PrivateJobUpdateParams setTags(List<String> tags) {
        super.setTags(tags);
        return this;
    }

    @Override
    public PrivateJobUpdateParams setVisited(Boolean visited) {
        super.setVisited(visited);
        return this;
    }

    @Override
    public PrivateJobUpdateParams setAttributes(Map<String, Object> attributes) {
        super.setAttributes(attributes);
        return this;
    }
}
