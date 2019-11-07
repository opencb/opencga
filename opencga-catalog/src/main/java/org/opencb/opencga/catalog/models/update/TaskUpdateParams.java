package org.opencb.opencga.catalog.models.update;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Status;

import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class TaskUpdateParams {

    private String commandLine;
    private String modificationDate;
    private Status status;
    private Map<String, String> params;

    public TaskUpdateParams() {
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
        final StringBuilder sb = new StringBuilder("TaskUpdateParams{");
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getCommandLine() {
        return commandLine;
    }

    public TaskUpdateParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public TaskUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public TaskUpdateParams setStatus(Status status) {
        this.status = status;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public TaskUpdateParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
