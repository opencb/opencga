package org.opencb.opencga.core.models.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.opencga.core.models.PrivateStudyUid;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class Pipeline extends PrivateStudyUid {

    private String id;
    private String uuid;
    private String description;

    private boolean disabled;
    private int version;

    private String creationDate;
    private String modificationDate;

    private Map<String, Object> params;
    private Options options;
    private List<JobDefinition> jobs;

    private static final String DEFAULT_PIPELINE_FORMAT = "yaml";

    public Pipeline() {
    }

    public Pipeline(String id, String uuid, String description, boolean disabled, int version, Map<String, Object> params, Options options,
                    List<JobDefinition> jobs) {
        this.id = id;
        this.uuid = uuid;
        this.description = description;
        this.disabled = disabled;
        this.version = version;
        this.params = params;
        this.options = options;
        this.jobs = jobs;
    }

    public static Pipeline load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, DEFAULT_PIPELINE_FORMAT);
    }

    public static Pipeline load(InputStream configurationInputStream, String format) throws IOException {
        if (configurationInputStream == null) {
            throw new IOException("Pipeline file not found");
        }
        Pipeline pipeline;
        ObjectMapper objectMapper;
        try {
            switch (format) {
                case "json":
                    objectMapper = new ObjectMapper();
                    pipeline = objectMapper.readValue(configurationInputStream, Pipeline.class);
                    break;
                case "yml":
                case "yaml":
                default:
                    objectMapper = new ObjectMapper(new YAMLFactory());
                    pipeline = objectMapper.readValue(configurationInputStream, Pipeline.class);
                    break;
            }
        } catch (IOException e) {
            throw new IOException("Pipeline file could not be parsed: " + e.getMessage(), e);
        }

        if (pipeline.getStudyUid() > 0) {
            throw new IllegalStateException("'studyUid' should be undefined");
        }
        if (pipeline.getUid() > 0) {
            throw new IllegalStateException("'uid' should be undefined");
        }

        return pipeline;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Pipeline{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", disabled=").append(disabled);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", params=").append(params);
        sb.append(", options=").append(options);
        sb.append(", jobs=").append(jobs);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Pipeline setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public Pipeline setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Pipeline setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public Pipeline setDisabled(boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Pipeline setVersion(int version) {
        this.version = version;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public Pipeline setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public Options getOptions() {
        return options;
    }

    public Pipeline setOptions(Options options) {
        this.options = options;
        return this;
    }

    public List<JobDefinition> getJobs() {
        return jobs;
    }

    public Pipeline setJobs(List<JobDefinition> jobs) {
        this.jobs = jobs;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Pipeline setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Pipeline setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public static class Options {

        private int retry;
        private int timeout;

        public Options() {
        }

        public Options(int retry, int timeout) {
            this.retry = retry;
            this.timeout = timeout;
        }

        public static Options init() {
            return new Options(3, 3600);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Options{");
            sb.append("retry=").append(retry);
            sb.append(", timeout=").append(timeout);
            sb.append('}');
            return sb.toString();
        }

        public int getRetry() {
            return retry;
        }

        public Options setRetry(int retry) {
            this.retry = retry;
            return this;
        }

        public int getTimeout() {
            return timeout;
        }

        public Options setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }
    }

    public static class JobDefinition {

        private String id;
        private String toolId;
        private String description;
        private Map<String, Object> params;
        private List<String> input;
        private List<String> output;
        private List<String> dependsOn;

        public JobDefinition() {
        }

        public JobDefinition(String id, String toolId, String description, Map<String, Object> params, List<String> input,
                             List<String> output, List<String> dependsOn) {
            this.id = id;
            this.toolId = toolId;
            this.description = description;
            this.params = params;
            this.input = input;
            this.output = output;
            this.dependsOn = dependsOn;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Job{");
            sb.append("id='").append(id).append('\'');
            sb.append(", toolId='").append(toolId).append('\'');
            sb.append(", description='").append(description).append('\'');
            sb.append(", params=").append(params);
            sb.append(", input=").append(input);
            sb.append(", output=").append(output);
            sb.append(", dependsOn=").append(dependsOn);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public JobDefinition setId(String id) {
            this.id = id;
            return this;
        }

        public String getToolId() {
            return toolId;
        }

        public JobDefinition setToolId(String toolId) {
            this.toolId = toolId;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public JobDefinition setDescription(String description) {
            this.description = description;
            return this;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public JobDefinition setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        public List<String> getInput() {
            return input;
        }

        public JobDefinition setInput(List<String> input) {
            this.input = input;
            return this;
        }

        public List<String> getOutput() {
            return output;
        }

        public JobDefinition setOutput(List<String> output) {
            this.output = output;
            return this;
        }

        public List<String> getDependsOn() {
            return dependsOn;
        }

        public JobDefinition setDependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn;
            return this;
        }
    }

}
