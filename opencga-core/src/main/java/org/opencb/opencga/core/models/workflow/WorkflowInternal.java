package org.opencb.opencga.core.models.workflow;

import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;

public class WorkflowInternal extends Internal {

    private String registrationUserId;

    public WorkflowInternal() {
    }

    public WorkflowInternal(InternalStatus status, String registrationDate, String lastModified, String registrationUserId) {
        super(status, registrationDate, lastModified);
        this.registrationUserId = registrationUserId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowInternal{");
        sb.append("registrationUserId='").append(registrationUserId).append('\'');
        sb.append(", status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getRegistrationUserId() {
        return registrationUserId;
    }

    public WorkflowInternal setRegistrationUserId(String registrationUserId) {
        this.registrationUserId = registrationUserId;
        return this;
    }
}
