package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.models.common.Enums;

import java.util.List;
import java.util.Map;

public class ExecutionCreateParams {

    private String id;
    private String description;

    private String userId;

    private String creationDate;
    private String modificationDate;

    private Enums.Priority priority;
    private ExecutionInternal internal;

    private Map<String, Object> params;

//    private File outDir;
    private List<String> tags;
    private List<EntryParam> dependsOn;

    private EntryParam pipeline;
    private List<JobCreateParams> jobs;
    private boolean visited;
    private Map<String, Object> attributes;

    public ExecutionCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", priority=").append(priority);
        sb.append(", internal=").append(internal);
        sb.append(", params=").append(params);
        sb.append(", tags=").append(tags);
        sb.append(", dependsOn=").append(dependsOn);
        sb.append(", pipeline=").append(pipeline);
        sb.append(", jobs=").append(jobs);
        sb.append(", visited=").append(visited);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ExecutionCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExecutionCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public ExecutionCreateParams setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ExecutionCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public ExecutionCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Enums.Priority getPriority() {
        return priority;
    }

    public ExecutionCreateParams setPriority(Enums.Priority priority) {
        this.priority = priority;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public ExecutionCreateParams setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public ExecutionCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public ExecutionInternal getInternal() {
        return internal;
    }

    public ExecutionCreateParams setInternal(ExecutionInternal internal) {
        this.internal = internal;
        return this;
    }

    public List<EntryParam> getDependsOn() {
        return dependsOn;
    }

    public ExecutionCreateParams setDependsOn(List<EntryParam> dependsOn) {
        this.dependsOn = dependsOn;
        return this;
    }

    public EntryParam getPipeline() {
        return pipeline;
    }

    public ExecutionCreateParams setPipeline(EntryParam pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public List<JobCreateParams> getJobs() {
        return jobs;
    }

    public ExecutionCreateParams setJobs(List<JobCreateParams> jobs) {
        this.jobs = jobs;
        return this;
    }

    public boolean isVisited() {
        return visited;
    }

    public ExecutionCreateParams setVisited(boolean visited) {
        this.visited = visited;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ExecutionCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
