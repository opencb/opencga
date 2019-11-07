package org.opencb.opencga.core.models;

import org.opencb.opencga.core.models.common.Enums;

import java.util.Map;

public class Task extends PrivateStudyUid {

    private String id;
    private String uuid;

    private String userId;
    private String commandLine;

    /**
     * Involved resource (User, Study, Sample, File...).
     */
    private Enums.Resource resource;
    /**
     * Action performed (CREATE, SEARCH, DOWNLOAD...).
     */
    private Enums.Action action;

    private String creationDate;
    private String modificationDate;

    private Enums.ExecutionStatus status;

    private Enums.Priority priority;

    private Map<String, String> params;

    public Task() {
    }

    public Task(String id, String uuid, String userId, String commandLine, Enums.Resource resource, Enums.Action action,
                String creationDate, String modificationDate, Enums.ExecutionStatus status, Enums.Priority priority, Map<String, String> params) {
        this.id = id;
        this.uuid = uuid;
        this.userId = userId;
        this.commandLine = commandLine;
        this.resource = resource;
        this.action = action;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.status = status;
        this.priority = priority;
        this.params = params;
    }

    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Task{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", resource=").append(resource);
        sb.append(", action=").append(action);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", priority=").append(priority);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Task setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public Task setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public Task setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public Task setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public Enums.Resource getResource() {
        return resource;
    }

    public Task setResource(Enums.Resource resource) {
        this.resource = resource;
        return this;
    }

    public Enums.Action getAction() {
        return action;
    }

    public Task setAction(Enums.Action action) {
        this.action = action;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Task setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Task setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Enums.ExecutionStatus getStatus() {
        return status;
    }

    public Task setStatus(Enums.ExecutionStatus status) {
        this.status = status;
        return this;
    }

    public Enums.Priority getPriority() {
        return priority;
    }

    public Task setPriority(Enums.Priority priority) {
        this.priority = priority;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Task setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
