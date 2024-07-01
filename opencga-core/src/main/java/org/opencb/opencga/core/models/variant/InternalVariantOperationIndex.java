package org.opencb.opencga.core.models.variant;

public class InternalVariantOperationIndex {

    private int numTries;
    private OperationIndexStatus status;

    public InternalVariantOperationIndex() {
        this(0, new OperationIndexStatus());
    }

    public InternalVariantOperationIndex(int numTries, OperationIndexStatus status) {
        this.numTries = numTries;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectInternalAnnotationIndex{");
        sb.append("numTries=").append(numTries);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public int getNumTries() {
        return numTries;
    }

    public InternalVariantOperationIndex setNumTries(int numTries) {
        this.numTries = numTries;
        return this;
    }

    public OperationIndexStatus getStatus() {
        return status;
    }

    public InternalVariantOperationIndex setStatus(OperationIndexStatus status) {
        this.status = status;
        return this;
    }
}
