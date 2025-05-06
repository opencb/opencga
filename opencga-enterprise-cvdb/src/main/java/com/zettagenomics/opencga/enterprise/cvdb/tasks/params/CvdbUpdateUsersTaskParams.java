package com.zettagenomics.opencga.enterprise.cvdb.tasks.params;

import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbUpdateUsersTask;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CvdbUpdateUsersTaskParams extends ToolParams {
    public static final String DESCRIPTION = "Parameters: " + CvdbUpdateUsersTask.DESCRIPTION;

    @DataField(id = "clinicalAnalysisIds", description = "List of clinical analyses to update users")
    private List<String> clinicalAnalysisIds;

    @DataField(id = "allProject", description = "Update users for all the clinical analyses of the given project")
    private boolean allProject;

    public CvdbUpdateUsersTaskParams() {
    }

    public CvdbUpdateUsersTaskParams(List<String> clinicalAnalysisIds, boolean allProject) {
        this.clinicalAnalysisIds = clinicalAnalysisIds;
        this.allProject = allProject;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbUpdateUsersTaskParams{");
        sb.append("clinicalAnalysisIds=").append(clinicalAnalysisIds);
        sb.append(", allProject=").append(allProject);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getClinicalAnalysisIds() {
        return clinicalAnalysisIds;
    }

    public CvdbUpdateUsersTaskParams setClinicalAnalysisIds(List<String> clinicalAnalysisIds) {
        this.clinicalAnalysisIds = clinicalAnalysisIds;
        return this;
    }

    public boolean isAllProject() {
        return allProject;
    }

    public CvdbUpdateUsersTaskParams setAllProject(boolean allProject) {
        this.allProject = allProject;
        return this;
    }
}
