package org.opencb.opencga.core.models.panel;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;

public class PanelInternal extends Internal {

    public PanelInternal() {
    }

    public PanelInternal(InternalStatus status, String registrationDate, String lastModified) {
        super(status, registrationDate, lastModified);
    }

    public static PanelInternal init() {
        return new PanelInternal(new InternalStatus(InternalStatus.READY), TimeUtils.getTime(), TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PanelInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public PanelInternal setStatus(InternalStatus status) {
        super.setStatus(status);
        return this;
    }

    @Override
    public PanelInternal setRegistrationDate(String registrationDate) {
        super.setRegistrationDate(registrationDate);
        return this;
    }

    @Override
    public PanelInternal setLastModified(String lastModified) {
        super.setLastModified(lastModified);
        return this;
    }
}
