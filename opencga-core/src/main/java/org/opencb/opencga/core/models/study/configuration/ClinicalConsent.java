package org.opencb.opencga.core.models.study.configuration;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalConsent {

    @DataField(description = ParamConstants.CLINICAL_CONSENT_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.CLINICAL_CONSENT_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    public ClinicalConsent() {
    }

    public ClinicalConsent(String id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalConsent{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalConsent setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalConsent setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalConsent setDescription(String description) {
        this.description = description;
        return this;
    }
}
