package org.opencb.opencga.clinical.cvdb.tasks.params;

import org.opencb.opencga.clinical.cvdb.tasks.CvdbUpdateAclTask;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CvdbUpdateAclTaskParams extends ToolParams {
    public static final String DESCRIPTION = "Parameters for the ACL updating task (" + CvdbUpdateAclTask.ID + ").";

    @DataField(id = "clinicalAnalysisIds", description = "List of clinical analyses to update ACLs.")
    private List<String> clinicalAnalysisIds;

    @DataField(id = "allProject", description = "Updates ACLs from all clinical analyses within the project.")
    private boolean allProject;

    public CvdbUpdateAclTaskParams() {
    }

    public CvdbUpdateAclTaskParams(List<String> clinicalAnalysisIds, boolean allProject) {
        this.clinicalAnalysisIds = clinicalAnalysisIds;
        this.allProject = allProject;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbUpdateAclTaskParams{");
        sb.append("clinicalAnalysisIds=").append(clinicalAnalysisIds);
        sb.append(", allProject=").append(allProject);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getClinicalAnalysisIds() {
        return clinicalAnalysisIds;
    }

    public CvdbUpdateAclTaskParams setClinicalAnalysisIds(List<String> clinicalAnalysisIds) {
        this.clinicalAnalysisIds = clinicalAnalysisIds;
        return this;
    }

    public boolean isAllProject() {
        return allProject;
    }

    public CvdbUpdateAclTaskParams setAllProject(boolean allProject) {
        this.allProject = allProject;
        return this;
    }
}
