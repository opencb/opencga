package org.opencb.opencga.core.models.study.configuration;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalConsentConfiguration {

    @DataField(description = ParamConstants.CLINICAL_CONSENT_CONFIGURATION_CONSENTS_DESCRIPTION)
    private List<ClinicalConsent> consents;

    public ClinicalConsentConfiguration() {
    }

    public ClinicalConsentConfiguration(List<ClinicalConsent> consents) {
        this.consents = consents;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalConsentConfiguration{");
        sb.append("consents=").append(consents);
        sb.append('}');
        return sb.toString();
    }

    public List<ClinicalConsent> getConsents() {
        return consents;
    }

    public ClinicalConsentConfiguration setConsents(List<ClinicalConsent> consents) {
        this.consents = consents;
        return this;
    }
}
