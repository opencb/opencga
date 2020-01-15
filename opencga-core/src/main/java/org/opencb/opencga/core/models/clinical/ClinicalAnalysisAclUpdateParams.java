package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.AclParams;

public class ClinicalAnalysisAclUpdateParams extends AclParams {

    private String clinicalAnalysis;

    public ClinicalAnalysisAclUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisAclUpdateParams{");
        sb.append("clinicalAnalysis='").append(clinicalAnalysis).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append('}');
        return sb.toString();
    }

    public String getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public ClinicalAnalysisAclUpdateParams setClinicalAnalysis(String clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
        return this;
    }
}
