package org.opencb.opencga.catalog.models.update;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.common.Enums;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class JobUpdateParams {

    private String id;
    private String name;

    private String commandLine;

    private Map<String, String> params;

    private Enums.ExecutionStatus status;

    private File outDir;
    private File tmpDir;
    private List<File> input;    // input files to this job
    private List<File> output;   // output files of this job
    private List<String> tags;

    private AnalysisResult result;

    private File log;
    private File errorLog;

    private Map<String, Object> attributes;

    public JobUpdateParams() {
    }

    public ObjectMap getUpdateMap() throws CatalogException {
        try {
            return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
        } catch (JsonProcessingException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", params=").append(params);
        sb.append(", status=").append(status);
        sb.append(", outDir=").append(outDir);
        sb.append(", tmpDir=").append(tmpDir);
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

    public JobUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public JobUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public JobUpdateParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public JobUpdateParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

    public Enums.ExecutionStatus getStatus() {
        return status;
    }

    public JobUpdateParams setStatus(Enums.ExecutionStatus status) {
        this.status = status;
        return this;
    }

    public File getOutDir() {
        return outDir;
    }

    public JobUpdateParams setOutDir(File outDir) {
        this.outDir = outDir;
        return this;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public JobUpdateParams setTmpDir(File tmpDir) {
        this.tmpDir = tmpDir;
        return this;
    }

    public List<File> getInput() {
        return input;
    }

    public JobUpdateParams setInput(List<File> input) {
        this.input = input;
        return this;
    }

    public List<File> getOutput() {
        return output;
    }

    public JobUpdateParams setOutput(List<File> output) {
        this.output = output;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public JobUpdateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public AnalysisResult getResult() {
        return result;
    }

    public JobUpdateParams setResult(AnalysisResult result) {
        this.result = result;
        return this;
    }

    public File getLog() {
        return log;
    }

    public JobUpdateParams setLog(File log) {
        this.log = log;
        return this;
    }

    public File getErrorLog() {
        return errorLog;
    }

    public JobUpdateParams setErrorLog(File errorLog) {
        this.errorLog = errorLog;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public JobUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
