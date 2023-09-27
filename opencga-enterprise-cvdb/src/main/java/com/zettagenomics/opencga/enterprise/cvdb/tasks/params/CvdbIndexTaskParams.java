package com.zettagenomics.opencga.enterprise.cvdb.tasks.params;

import org.opencb.opencga.core.tools.ToolParams;

public class CvdbIndexTaskParams extends ToolParams {
    public static final String DESCRIPTION = "Parameters to index clinical analysis into CVDB";

    private String projectId;
    private boolean overwrite;

    public CvdbIndexTaskParams() {
    }

    public CvdbIndexTaskParams(String projectId, boolean overwrite) {
        this.projectId = projectId;
        this.overwrite = overwrite;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbIndexTaskParams{");
        sb.append("projectId='").append(projectId).append('\'');
        sb.append(", overwrite=").append(overwrite);
        sb.append('}');
        return sb.toString();
    }

    public String getProjectId() {
        return projectId;
    }

    public CvdbIndexTaskParams setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public CvdbIndexTaskParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
