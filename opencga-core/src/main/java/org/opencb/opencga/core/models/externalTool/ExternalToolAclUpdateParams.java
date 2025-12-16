package org.opencb.opencga.core.models.externalTool;

import java.util.List;

public class ExternalToolAclUpdateParams {

    private List<String> externalToolIds;
    private List<String> permissions;

    public ExternalToolAclUpdateParams() {
    }

    public ExternalToolAclUpdateParams(List<String> externalToolIds, List<String> permissions) {
        this.externalToolIds = externalToolIds;
        this.permissions = permissions;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowAclUpdateParams{");
        sb.append("externalToolIds=").append(externalToolIds);
        sb.append(", permissions=").append(permissions);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getExternalToolIds() {
        return externalToolIds;
    }

    public ExternalToolAclUpdateParams setExternalToolIds(List<String> externalToolIds) {
        this.externalToolIds = externalToolIds;
        return this;
    }

    public List<String> getPermissions() {
        return permissions;
    }

    public ExternalToolAclUpdateParams setPermissions(List<String> permissions) {
        this.permissions = permissions;
        return this;
    }
}
