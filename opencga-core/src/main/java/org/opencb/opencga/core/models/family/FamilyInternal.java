package org.opencb.opencga.core.models.family;

public class FamilyInternal {

    private FamilyStatus status;

    public FamilyInternal() {
    }

    public FamilyInternal(FamilyStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public FamilyStatus getStatus() {
        return status;
    }

    public FamilyInternal setStatus(FamilyStatus status) {
        this.status = status;
        return this;
    }
}
