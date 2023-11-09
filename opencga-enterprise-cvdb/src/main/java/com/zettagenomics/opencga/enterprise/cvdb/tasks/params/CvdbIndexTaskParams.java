package com.zettagenomics.opencga.enterprise.cvdb.tasks.params;

import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbIndexTask;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CvdbIndexTaskParams extends ToolParams {
    public static final String DESCRIPTION = "Parameters: " + CvdbIndexTask.DESCRIPTION;

    private String projectId;
    private String studyId;
    private List<String> clinicalAnalysisIds;
    private boolean overwrite;

    public CvdbIndexTaskParams() {
    }

    public CvdbIndexTaskParams(String projectId, String studyId, List<String> clinicalAnalysisIds, boolean overwrite) {
        this.projectId = projectId;
        this.studyId = studyId;
        this.clinicalAnalysisIds = clinicalAnalysisIds;
        this.overwrite = overwrite;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbIndexTaskParams{");
        sb.append("projectId='").append(projectId).append('\'');
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", clinicalAnalysisIds=").append(clinicalAnalysisIds);
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

    public String getStudyId() {
        return studyId;
    }

    public CvdbIndexTaskParams setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public List<String> getClinicalAnalysisIds() {
        return clinicalAnalysisIds;
    }

    public CvdbIndexTaskParams setClinicalAnalysisIds(List<String> clinicalAnalysisIds) {
        this.clinicalAnalysisIds = clinicalAnalysisIds;
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
