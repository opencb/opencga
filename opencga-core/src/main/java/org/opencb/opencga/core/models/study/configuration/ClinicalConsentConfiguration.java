package org.opencb.opencga.core.models.study.configuration;

import java.util.List;

public class ClinicalConsentConfiguration {

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
