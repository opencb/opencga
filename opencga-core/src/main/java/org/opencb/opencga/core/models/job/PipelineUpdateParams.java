package org.opencb.opencga.core.models.job;

import java.util.LinkedHashMap;
import java.util.Map;

public class PipelineUpdateParams {

    private String description;
    private Boolean disabled;

    private String creationDate;
    private String modificationDate;

    private Map<String, Object> params;
    private Pipeline.PipelineConfig config;
    private LinkedHashMap<String, Pipeline.PipelineJob> jobs;

    public PipelineUpdateParams() {
    }

    public PipelineUpdateParams(String description, boolean disabled, String creationDate, String modificationDate,
                                Map<String, Object> params, Pipeline.PipelineConfig config,
                                LinkedHashMap<String, Pipeline.PipelineJob> jobs) {
        this.description = description;
        this.disabled = disabled;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.params = params;
        this.config = config;
        this.jobs = jobs;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineUpdateParams{");
        sb.append("description='").append(description).append('\'');
        sb.append(", disabled=").append(disabled);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", params=").append(params);
        sb.append(", config=").append(config);
        sb.append(", jobs=").append(jobs);
        sb.append('}');
        return sb.toString();
    }

    public String getDescription() {
        return description;
    }

    public PipelineUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public Boolean isDisabled() {
        return disabled;
    }

    public PipelineUpdateParams setDisabled(Boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public PipelineUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public PipelineUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public PipelineUpdateParams setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public Pipeline.PipelineConfig getConfig() {
        return config;
    }

    public PipelineUpdateParams setConfig(Pipeline.PipelineConfig config) {
        this.config = config;
        return this;
    }

    public LinkedHashMap<String, Pipeline.PipelineJob> getJobs() {
        return jobs;
    }

    public PipelineUpdateParams setJobs(LinkedHashMap<String, Pipeline.PipelineJob> jobs) {
        this.jobs = jobs;
        return this;
    }
}
