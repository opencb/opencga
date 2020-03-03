package org.opencb.opencga.core.models.clinical;

public class ClinicalAnalysisInternal {

    private ClinicalAnalysisStatus status;

    public ClinicalAnalysisInternal() {
    }

    public ClinicalAnalysisInternal(ClinicalAnalysisStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public ClinicalAnalysisStatus getStatus() {
        return status;
    }

    public ClinicalAnalysisInternal setStatus(ClinicalAnalysisStatus status) {
        this.status = status;
        return this;
    }
}
