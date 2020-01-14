package org.opencb.opencga.core.models.clinical;

public class ClinicalConsent {

    private ConsentStatus primaryFindings;
    private ConsentStatus secondaryFindings;
    private ConsentStatus carrierFindings;
    private ConsentStatus researchFindings;

    public enum ConsentStatus {
        YES, NO, UNKNOWN
    }

    public ClinicalConsent() {
        this(ConsentStatus.UNKNOWN, ConsentStatus.UNKNOWN, ConsentStatus.UNKNOWN, ConsentStatus.UNKNOWN);
    }

    public ClinicalConsent(ConsentStatus primaryFindings, ConsentStatus secondaryFindings, ConsentStatus carrierFindings,
                           ConsentStatus researchFindings) {
        this.primaryFindings = primaryFindings != null ? primaryFindings : ConsentStatus.UNKNOWN;
        this.secondaryFindings = secondaryFindings != null ? secondaryFindings : ConsentStatus.UNKNOWN;
        this.carrierFindings = carrierFindings != null ? carrierFindings : ConsentStatus.UNKNOWN;
        this.researchFindings = researchFindings != null ? researchFindings : ConsentStatus.UNKNOWN;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalConsent{");
        sb.append("primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append(", carrierFindings=").append(carrierFindings);
        sb.append(", researchFindings=").append(researchFindings);
        sb.append('}');
        return sb.toString();
    }


    public ConsentStatus getPrimaryFindings() {
        return primaryFindings;
    }

    public ClinicalConsent setPrimaryFindings(ConsentStatus primaryFindings) {
        this.primaryFindings = primaryFindings;
        return this;
    }

    public ConsentStatus getSecondaryFindings() {
        return secondaryFindings;
    }

    public ClinicalConsent setSecondaryFindings(ConsentStatus secondaryFindings) {
        this.secondaryFindings = secondaryFindings;
        return this;
    }

    public ConsentStatus getCarrierFindings() {
        return carrierFindings;
    }

    public ClinicalConsent setCarrierFindings(ConsentStatus carrierFindings) {
        this.carrierFindings = carrierFindings;
        return this;
    }

    public ConsentStatus getResearchFindings() {
        return researchFindings;
    }

    public ClinicalConsent setResearchFindings(ConsentStatus researchFindings) {
        this.researchFindings = researchFindings;
        return this;
    }
}
