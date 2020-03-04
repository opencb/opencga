package org.opencb.opencga.core.models.cohort;

public class CohortInternal {

    private CohortStatus status;

    public CohortInternal() {
    }

    public CohortInternal(CohortStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public CohortStatus getStatus() {
        return status;
    }

    public CohortInternal setStatus(CohortStatus status) {
        this.status = status;
        return this;
    }
}
