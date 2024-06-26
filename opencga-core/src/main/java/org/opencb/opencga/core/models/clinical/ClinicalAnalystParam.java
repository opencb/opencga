package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalAnalyst;

import java.util.Collections;

public class ClinicalAnalystParam {

    private String id;

    public ClinicalAnalystParam() {
    }

    public ClinicalAnalystParam(String id) {
        this.id = id;
    }

    public static ClinicalAnalystParam of(ClinicalAnalyst clinicalAnalyst) {
        if (clinicalAnalyst != null) {
            return new ClinicalAnalystParam(clinicalAnalyst.getId());
        } else {
            return new ClinicalAnalystParam("");
        }
    }

    public ClinicalAnalyst toClinicalAnalyst() {
        return new ClinicalAnalyst(id != null ? id : "", "", "", "", Collections.emptyMap());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalystParam{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalAnalystParam setId(String id) {
        this.id = id;
        return this;
    }
}
