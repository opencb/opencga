package org.opencb.opencga.core.models.admin;

public class WorkspaceUpdateParams {

    public String oldWorkspace;
    public String newWorkspace;

    public WorkspaceUpdateParams() {
    }

    public WorkspaceUpdateParams(String oldWorkspace, String newWorkspace) {
        this.oldWorkspace = oldWorkspace;
        this.newWorkspace = newWorkspace;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkspaceUpdateParams{");
        sb.append("oldWorkspace='").append(oldWorkspace).append('\'');
        sb.append(", newWorkspace='").append(newWorkspace).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getOldWorkspace() {
        return oldWorkspace;
    }

    public WorkspaceUpdateParams setOldWorkspace(String oldWorkspace) {
        this.oldWorkspace = oldWorkspace;
        return this;
    }

    public String getNewWorkspace() {
        return newWorkspace;
    }

    public WorkspaceUpdateParams setNewWorkspace(String newWorkspace) {
        this.newWorkspace = newWorkspace;
        return this;
    }
}
