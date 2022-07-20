package org.opencb.opencga.core.models.study.configuration;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalConsentParam {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;
    @DataField(id = "name", indexed = true,
            description = FieldConstants.GENERIC_NAME)
    private String name;

    @DataField(id = "description", indexed = true,
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "value", indexed = true, uncommentedClasses = {"Value"},
            description = FieldConstants.CLINICAL_CONSENT_PARAM_VALUE)
    private Value value;

    public ClinicalConsentParam() {
    }

    public ClinicalConsentParam(String id, String name, String description, Value value) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalConsent{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalConsentParam setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalConsentParam setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalConsentParam setDescription(String description) {
        this.description = description;
        return this;
    }

    public Value getValue() {
        return value;
    }

    public ClinicalConsentParam setValue(Value value) {
        this.value = value;
        return this;
    }

    public enum Value {
        YES,
        NO,
        UNKNOWN
    }
}
