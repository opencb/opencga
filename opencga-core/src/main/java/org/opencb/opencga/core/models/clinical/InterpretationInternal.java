package org.opencb.opencga.core.models.clinical;

public class InterpretationInternal {

    private InterpretationStatus status;

    public InterpretationInternal() {
    }

    public InterpretationInternal(InterpretationStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public InterpretationStatus getStatus() {
        return status;
    }

    public InterpretationInternal setStatus(InterpretationStatus status) {
        this.status = status;
        return this;
    }
}
