package org.opencb.opencga.core.models.study.configuration;

import java.util.List;

public class ClinicalConsentAnnotation {

    private List<ClinicalConsentParam> consents;
    private String date;

    public ClinicalConsentAnnotation() {
    }

    public ClinicalConsentAnnotation(List<ClinicalConsentParam> consents, String date) {
        this.consents = consents;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalConsentAnnotation{");
        sb.append("consents=").append(consents);
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<ClinicalConsentParam> getConsents() {
        return consents;
    }

    public ClinicalConsentAnnotation setConsents(List<ClinicalConsentParam> consents) {
        this.consents = consents;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalConsentAnnotation setDate(String date) {
        this.date = date;
        return this;
    }
}
