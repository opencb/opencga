package org.opencb.opencga.core.models.study.configuration;

import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.FlagValue;
import org.opencb.opencga.core.models.common.StatusValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClinicalStudyConfiguration {

    private Map<ClinicalAnalysis.Type, List<StatusValue>> status;
    private Map<ClinicalAnalysis.Type, List<StatusValue>> interpretationStatus;

    private List<ClinicalPriorityValue> priorities;
    private Map<ClinicalAnalysis.Type, List<FlagValue>> flags;
    private List<ClinicalConsent> consents;


    public ClinicalStudyConfiguration() {
    }

    public ClinicalStudyConfiguration(Map<ClinicalAnalysis.Type, List<StatusValue>> status,
                                      Map<ClinicalAnalysis.Type, List<StatusValue>> interpretationStatus,
                                      List<ClinicalPriorityValue> priorities, Map<ClinicalAnalysis.Type, List<FlagValue>> flags,
                                      List<ClinicalConsent> consents) {
        this.status = status;
        this.interpretationStatus = interpretationStatus;
        this.priorities = priorities;
        this.flags = flags;
        this.consents = consents;
    }

    public static ClinicalStudyConfiguration defaultConfiguration() {
        Map<ClinicalAnalysis.Type, List<StatusValue>> status = new HashMap<>();
        Map<ClinicalAnalysis.Type, List<StatusValue>> interpretationStatus = new HashMap<>();
        List<ClinicalPriorityValue> priorities = new ArrayList<>(5);
        Map<ClinicalAnalysis.Type, List<FlagValue>> flags = new HashMap<>();
        List<ClinicalConsent> clinicalConsentList = new ArrayList<>();

        List<StatusValue> statusValueList = new ArrayList<>(4);
        statusValueList.add(new StatusValue("READY_FOR_INTERPRETATION", "The Clinical Analysis is ready for interpretations"));
        statusValueList.add(new StatusValue("READY_FOR_REPORT", "The Interpretation is finished and it is to create the report"));
        statusValueList.add(new StatusValue("CLOSED", "The Clinical Analysis is closed"));
        statusValueList.add(new StatusValue("REJECTED", "The Clinical Analysis is rejected"));
        status.put(ClinicalAnalysis.Type.FAMILY, statusValueList);
        status.put(ClinicalAnalysis.Type.AUTOCOMPARATIVE, statusValueList);
        status.put(ClinicalAnalysis.Type.CANCER, statusValueList);
        status.put(ClinicalAnalysis.Type.COHORT, statusValueList);
        status.put(ClinicalAnalysis.Type.SINGLE, statusValueList);

        List<StatusValue> interpretationStatusList = new ArrayList<>(3);
        interpretationStatusList.add(new StatusValue("IN_PROGRESS", "Interpretation in progress"));
        interpretationStatusList.add(new StatusValue("READY", "Interpretation ready"));
        interpretationStatusList.add(new StatusValue("REJECTED", "Interpretation rejected"));
        interpretationStatus.put(ClinicalAnalysis.Type.FAMILY, interpretationStatusList);
        interpretationStatus.put(ClinicalAnalysis.Type.AUTOCOMPARATIVE, interpretationStatusList);
        interpretationStatus.put(ClinicalAnalysis.Type.CANCER, interpretationStatusList);
        interpretationStatus.put(ClinicalAnalysis.Type.COHORT, interpretationStatusList);
        interpretationStatus.put(ClinicalAnalysis.Type.SINGLE, interpretationStatusList);

        priorities.add(new ClinicalPriorityValue("URGENT", "Highest priority of all", 1));
        priorities.add(new ClinicalPriorityValue("HIGH", "Second highest priority of all", 2));
        priorities.add(new ClinicalPriorityValue("MEDIUM", "Intermediate priority", 3));
        priorities.add(new ClinicalPriorityValue("LOW", "Low priority", 4));
        priorities.add(new ClinicalPriorityValue("UNKNOWN", "Unknown priority. Treated as the lowest priority of all.", 5));

        List<FlagValue> flagValueList = new ArrayList<>(7);
        flagValueList.add(new FlagValue("MIXED_CHEMISTRIES", ""));
        flagValueList.add(new FlagValue("LOW_TUMOUR_PURITY", ""));
        flagValueList.add(new FlagValue("UNIPARENTAL_ISODISOMY", ""));
        flagValueList.add(new FlagValue("UNIPARENTAL_HETERODISOMY", ""));
        flagValueList.add(new FlagValue("UNUSUAL_KARYOTYPE", ""));
        flagValueList.add(new FlagValue("SUSPECTED_MOSAICISM", ""));
        flagValueList.add(new FlagValue("LOW_QUALITY_SAMPLE", ""));
        flags.put(ClinicalAnalysis.Type.FAMILY, flagValueList);
        flags.put(ClinicalAnalysis.Type.AUTOCOMPARATIVE, flagValueList);
        flags.put(ClinicalAnalysis.Type.CANCER, flagValueList);
        flags.put(ClinicalAnalysis.Type.COHORT, flagValueList);
        flags.put(ClinicalAnalysis.Type.SINGLE, flagValueList);

        clinicalConsentList.add(new ClinicalConsent("PRIMARY_FINDINGS", "Primary findings", "", ClinicalConsent.Value.UNKNOWN));
        clinicalConsentList.add(new ClinicalConsent("SECONDARY_FINDINGS", "Secondary findings", "", ClinicalConsent.Value.UNKNOWN));
        clinicalConsentList.add(new ClinicalConsent("CARRIER_FINDINGS", "Carrier findings", "", ClinicalConsent.Value.UNKNOWN));
        clinicalConsentList.add(new ClinicalConsent("RESEARCH_FINDINGS", "Research findings", "", ClinicalConsent.Value.UNKNOWN));

        return new ClinicalStudyConfiguration(status, interpretationStatus, priorities, flags, clinicalConsentList);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalStudyConfiguration{");
        sb.append("status=").append(status);
        sb.append(", interpretationStatus=").append(interpretationStatus);
        sb.append(", priorities=").append(priorities);
        sb.append(", flags=").append(flags);
        sb.append(", consents=").append(consents);
        sb.append('}');
        return sb.toString();
    }

    public Map<ClinicalAnalysis.Type, List<StatusValue>> getStatus() {
        return status;
    }

    public ClinicalStudyConfiguration setStatus(Map<ClinicalAnalysis.Type, List<StatusValue>> status) {
        this.status = status;
        return this;
    }

    public Map<ClinicalAnalysis.Type, List<StatusValue>> getInterpretationStatus() {
        return interpretationStatus;
    }

    public ClinicalStudyConfiguration setInterpretationStatus(Map<ClinicalAnalysis.Type, List<StatusValue>> interpretationStatus) {
        this.interpretationStatus = interpretationStatus;
        return this;
    }

    public List<ClinicalPriorityValue> getPriorities() {
        return priorities;
    }

    public ClinicalStudyConfiguration setPriorities(List<ClinicalPriorityValue> priorities) {
        this.priorities = priorities;
        return this;
    }

    public Map<ClinicalAnalysis.Type, List<FlagValue>> getFlags() {
        return flags;
    }

    public ClinicalStudyConfiguration setFlags(Map<ClinicalAnalysis.Type, List<FlagValue>> flags) {
        this.flags = flags;
        return this;
    }

    public List<ClinicalConsent> getConsents() {
        return consents;
    }

    public ClinicalStudyConfiguration setConsents(List<ClinicalConsent> consents) {
        this.consents = consents;
        return this;
    }
}
