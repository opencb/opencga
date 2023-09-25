package com.zettagenomics.opencga.enterprise.cvdb.tasks.params;

import org.opencb.opencga.core.tools.ToolParams;

public class CvdbIndexTaskParams extends ToolParams {
    public static final String DESCRIPTION = "Parameters to index clinical analysis into CVDB";

    private String project;

    public CvdbIndexTaskParams() {
    }

    public CvdbIndexTaskParams(String project) {
        this.project = project;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbIndexParams{");
        sb.append("project=").append(project);
        sb.append('}');
        return sb.toString();
    }

    public String getProject() {
        return project;
    }

    public CvdbIndexTaskParams setProject(String project) {
        this.project = project;
        return this;
    }
}
