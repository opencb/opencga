package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;

public class PipelineInternal extends Internal {

    public PipelineInternal() {
    }

    public PipelineInternal(InternalStatus status, String registrationDate, String lastModified) {
        super(status, registrationDate, lastModified);
    }

    public static PipelineInternal init() {
        return new PipelineInternal(new InternalStatus(InternalStatus.READY), TimeUtils.getTime(), TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public InternalStatus getStatus() {
        return super.getStatus();
    }

    @Override
    public PipelineInternal setStatus(InternalStatus status) {
        super.setStatus(status);
        return this;
    }

    @Override
    public String getRegistrationDate() {
        return super.getRegistrationDate();
    }

    @Override
    public PipelineInternal setRegistrationDate(String registrationDate) {
        super.setRegistrationDate(registrationDate);
        return this;
    }

    @Override
    public String getLastModified() {
        return super.getLastModified();
    }

    @Override
    public PipelineInternal setLastModified(String lastModified) {
        super.setLastModified(lastModified);
        return this;
    }
}
