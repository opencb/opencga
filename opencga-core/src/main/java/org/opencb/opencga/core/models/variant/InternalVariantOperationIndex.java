package org.opencb.opencga.core.models.variant;

public class InternalVariantOperationIndex {

    private OperationIndexStatus status;

    public InternalVariantOperationIndex() {
        this(new OperationIndexStatus());
    }

    public InternalVariantOperationIndex(OperationIndexStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectInternalAnnotationIndex{");
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public OperationIndexStatus getStatus() {
        return status;
    }

    public InternalVariantOperationIndex setStatus(OperationIndexStatus status) {
        this.status = status;
        return this;
    }
}
