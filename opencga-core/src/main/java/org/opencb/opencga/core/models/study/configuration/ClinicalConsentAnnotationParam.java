package org.opencb.opencga.core.models.study.configuration;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalConsentAnnotationParam {

    private List<ClinicalConsentParam> consents;

    public ClinicalConsentAnnotationParam() {
    }

    public ClinicalConsentAnnotationParam(List<ClinicalConsentParam> consents) {
        this.consents = consents;
    }

    public static ClinicalConsentAnnotationParam of(ClinicalConsentAnnotation clinicalConsent) {
        if (clinicalConsent == null) {
            return null;
        } else {
            List<ClinicalConsentParam> consents = clinicalConsent.getConsents()
                    .stream()
                    .map(ClinicalConsentAnnotationParam.ClinicalConsentParam::of)
                    .collect(Collectors.toList());
            return new ClinicalConsentAnnotationParam(consents);
        }
    }

    public ClinicalConsentAnnotation toClinicalConsentAnnotation() {
        if (consents != null) {
            List<org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam> consentList = consents.stream()
                    .map(ClinicalConsentAnnotationParam.ClinicalConsentParam::toClinicalConsent)
                    .collect(Collectors.toList());
            return new ClinicalConsentAnnotation(consentList, TimeUtils.getTime());
        } else {
            return new ClinicalConsentAnnotation(new LinkedList<>(), TimeUtils.getTime());
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalConsentAnnotationParam{");
        sb.append("consents=").append(consents);
        sb.append('}');
        return sb.toString();
    }

    public static class ClinicalConsentParam {
        private String id;
        private org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam.Value value;

        public ClinicalConsentParam() {
        }

        public ClinicalConsentParam(String id, org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam.Value value) {
            this.id = id;
            this.value = value;
        }

        public static ClinicalConsentParam of(org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam clinicalConsent) {
            return new ClinicalConsentParam(clinicalConsent.getId(), clinicalConsent.getValue());
        }

        public org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam toClinicalConsent() {
            return new org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam(id, "", "", value);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ClinicalConsentParam{");
            sb.append("id='").append(id).append('\'');
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

        public org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam.Value getValue() {
            return value;
        }

        public ClinicalConsentParam setValue(org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam.Value value) {
            this.value = value;
            return this;
        }
    }

    public List<ClinicalConsentParam> getConsents() {
        return consents;
    }

    public ClinicalConsentAnnotationParam setConsents(List<ClinicalConsentParam> consents) {
        this.consents = consents;
        return this;
    }
}
