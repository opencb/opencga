package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.configuration.ClinicalPriorityAnnotation;

public class PriorityParam {

    private String id;

    public PriorityParam() {
    }

    public PriorityParam(String id) {
        this.id = id;
    }

    public static PriorityParam of(ClinicalPriorityAnnotation priorityAnnotation) {
        return priorityAnnotation != null ? new PriorityParam(priorityAnnotation.getId()) : null;
    }

    public ClinicalPriorityAnnotation toClinicalPriorityAnnotation() {
        return new ClinicalPriorityAnnotation(id, "", 0, TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PriorityParam{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public PriorityParam setId(String id) {
        this.id = id;
        return this;
    }
}
