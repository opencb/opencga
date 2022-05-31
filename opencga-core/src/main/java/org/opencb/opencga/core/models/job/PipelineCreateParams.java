package org.opencb.opencga.core.models.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class PipelineCreateParams {

    private String id;
    private String description;

    private boolean disabled;

    private String creationDate;
    private String modificationDate;

    private Map<String, Object> params;
    private Pipeline.PipelineConfig config;
    private LinkedHashMap<String, Pipeline.PipelineJob> jobs;

    private static final String DEFAULT_FORMAT = "yaml";

    public PipelineCreateParams() {
    }

    public static PipelineCreateParams load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, DEFAULT_FORMAT);
    }

    public static PipelineCreateParams load(InputStream configurationInputStream, String format) throws IOException {
        if (configurationInputStream == null) {
            throw new IOException("Pipeline file not found");
        }
        PipelineCreateParams pipeline;
        ObjectMapper objectMapper;
        try {
            switch (format) {
                case "json":
                    objectMapper = new ObjectMapper();
                    pipeline = objectMapper.readValue(configurationInputStream, PipelineCreateParams.class);
                    break;
                case "yml":
                case "yaml":
                default:
                    objectMapper = new ObjectMapper(new YAMLFactory());
                    pipeline = objectMapper.readValue(configurationInputStream, PipelineCreateParams.class);
                    break;
            }
        } catch (IOException e) {
            throw new IOException("Pipeline file could not be parsed: " + e.getMessage(), e);
        }

        return pipeline;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", disabled=").append(disabled);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", params=").append(params);
        sb.append(", config=").append(config);
        sb.append(", jobs=").append(jobs);
        sb.append('}');
        return sb.toString();
    }

    public Pipeline toPipeline() {
        return new Pipeline(id, description, disabled, creationDate, modificationDate, null, params, config, jobs);
    }

    public String getId() {
        return id;
    }

    public PipelineCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public PipelineCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public PipelineCreateParams setDisabled(boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public PipelineCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public PipelineCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public PipelineCreateParams setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public Pipeline.PipelineConfig getConfig() {
        return config;
    }

    public PipelineCreateParams setConfig(Pipeline.PipelineConfig config) {
        this.config = config;
        return this;
    }

    public LinkedHashMap<String, Pipeline.PipelineJob> getJobs() {
        return jobs;
    }

    public PipelineCreateParams setJobs(LinkedHashMap<String, Pipeline.PipelineJob> jobs) {
        this.jobs = jobs;
        return this;
    }
}
