package org.opencb.opencga.core.models.externalTool;

import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;

public class ExternalToolInternal extends Internal {

    private String registrationUserId;

    public ExternalToolInternal() {
    }

    public ExternalToolInternal(InternalStatus status, String registrationDate, String lastModified, String registrationUserId) {
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

    public ExternalToolInternal setRegistrationUserId(String registrationUserId) {
        this.registrationUserId = registrationUserId;
        return this;
    }
}
