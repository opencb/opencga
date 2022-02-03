package org.opencb.opencga.core.models.common;

import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.common.TimeUtils;

public class StatusParam {

    private String id;

    public StatusParam() {
    }

    public StatusParam(String id) {
        this.id = id;
    }

    public static StatusParam of(Status status) {
        return status != null ? new StatusParam(status.getId()) : null;
    }

    public Status toStatus() {
        return new Status(id, "", "", TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StatusParam{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public StatusParam setId(String id) {
        this.id = id;
        return this;
    }
}
