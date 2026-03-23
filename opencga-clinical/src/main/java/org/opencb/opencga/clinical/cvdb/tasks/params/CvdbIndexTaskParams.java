package org.opencb.opencga.clinical.cvdb.tasks.params;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CvdbIndexTaskParams extends ToolParams {

    @DataField(id = "clinicalAnalysisIds", description = "List of clinical analyses, separated by commas, for a specific study. To index"
            + " all clinical analyses for a given study, leave this parameter empty")
    private List<String> clinicalAnalysisIds;

    @DataField(id = "allProject", description = "Index all the clinical analyses of the given project")
    private boolean allProject;

    @DataField(id = "overwrite", description = "Overwrite clinical analyses already indexed")
    private boolean overwrite;

    public CvdbIndexTaskParams() {
    }

    public CvdbIndexTaskParams(List<String> clinicalAnalysisIds, boolean allProject, boolean overwrite) {
        this.clinicalAnalysisIds = clinicalAnalysisIds;
        this.allProject = allProject;
        this.overwrite = overwrite;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbIndexTaskParams{");
        sb.append("clinicalAnalysisIds=").append(clinicalAnalysisIds);
        sb.append(", allProject=").append(allProject);
        sb.append(", overwrite=").append(overwrite);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getClinicalAnalysisIds() {
        return clinicalAnalysisIds;
    }

    public CvdbIndexTaskParams setClinicalAnalysisIds(List<String> clinicalAnalysisIds) {
        this.clinicalAnalysisIds = clinicalAnalysisIds;
        return this;
    }

    public boolean isAllProject() {
        return allProject;
    }

    public CvdbIndexTaskParams setAllProject(boolean allProject) {
        this.allProject = allProject;
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
