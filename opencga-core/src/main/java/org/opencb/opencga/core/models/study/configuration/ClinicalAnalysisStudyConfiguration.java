package org.opencb.opencga.core.models.study.configuration;

import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.FlagValue;
import org.opencb.opencga.core.models.clinical.ClinicalStatusValue;

import java.util.*;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalAnalysisStudyConfiguration {

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> status;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_STUDY_CONFIGURATION_INTERPRETATION_DESCRIPTION)
    private InterpretationStudyConfiguration interpretation;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_STUDY_CONFIGURATION_PRIORITIES_DESCRIPTION)
    private List<ClinicalPriorityValue> priorities;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_STUDY_CONFIGURATION_FLAGS_DESCRIPTION)
    private Map<ClinicalAnalysis.Type, List<FlagValue>> flags;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_STUDY_CONFIGURATION_CONSENT_DESCRIPTION)
    private ClinicalConsentConfiguration consent;


    public ClinicalAnalysisStudyConfiguration() {
    }

    public ClinicalAnalysisStudyConfiguration(Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> status,
                                              InterpretationStudyConfiguration interpretation, List<ClinicalPriorityValue> priorities,
                                              Map<ClinicalAnalysis.Type, List<FlagValue>> flags, ClinicalConsentConfiguration consent) {
        this.status = status;
        this.interpretation = interpretation;
        this.priorities = priorities;
        this.flags = flags;
        this.consent = consent;
    }

    public static ClinicalAnalysisStudyConfiguration defaultConfiguration() {
        Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> status = new HashMap<>();
        Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> interpretationStatus = new HashMap<>();
        List<ClinicalPriorityValue> priorities = new ArrayList<>(5);
        Map<ClinicalAnalysis.Type, List<FlagValue>> flags = new HashMap<>();
        List<ClinicalConsent> clinicalConsentList = new ArrayList<>();

        List<ClinicalStatusValue> clinicalStatusValueList = new ArrayList<>(4);
        clinicalStatusValueList.add(
                new ClinicalStatusValue("READY_FOR_INTERPRETATION", "The Clinical Analysis is ready for interpretations",
                        ClinicalStatusValue.ClinicalStatusType.NOT_STARTED)
        );
        clinicalStatusValueList.add(
                new ClinicalStatusValue("READY_FOR_REPORT", "The Interpretation is finished and it is to create the report",
                        ClinicalStatusValue.ClinicalStatusType.IN_PROGRESS)
        );
        clinicalStatusValueList.add(
                new ClinicalStatusValue("CLOSED", "The Clinical Analysis is closed", ClinicalStatusValue.ClinicalStatusType.CLOSED)
        );
        clinicalStatusValueList.add(
                new ClinicalStatusValue("REJECTED", "The Clinical Analysis is rejected", ClinicalStatusValue.ClinicalStatusType.CLOSED)
        );
        status.put(ClinicalAnalysis.Type.FAMILY, clinicalStatusValueList);
        status.put(ClinicalAnalysis.Type.AUTOCOMPARATIVE, clinicalStatusValueList);
        status.put(ClinicalAnalysis.Type.CANCER, clinicalStatusValueList);
        status.put(ClinicalAnalysis.Type.COHORT, clinicalStatusValueList);
        status.put(ClinicalAnalysis.Type.SINGLE, clinicalStatusValueList);

        List<ClinicalStatusValue> interpretationStatusList = new ArrayList<>(3);
        interpretationStatusList.add(new ClinicalStatusValue("IN_PROGRESS", "Interpretation in progress", ClinicalStatusValue.ClinicalStatusType.IN_PROGRESS));
        interpretationStatusList.add(new ClinicalStatusValue("READY", "Interpretation ready", ClinicalStatusValue.ClinicalStatusType.CLOSED));
        interpretationStatusList.add(new ClinicalStatusValue("REJECTED", "Interpretation rejected", ClinicalStatusValue.ClinicalStatusType.CLOSED));
        interpretationStatus.put(ClinicalAnalysis.Type.FAMILY, interpretationStatusList);
        interpretationStatus.put(ClinicalAnalysis.Type.AUTOCOMPARATIVE, interpretationStatusList);
        interpretationStatus.put(ClinicalAnalysis.Type.CANCER, interpretationStatusList);
        interpretationStatus.put(ClinicalAnalysis.Type.COHORT, interpretationStatusList);
        interpretationStatus.put(ClinicalAnalysis.Type.SINGLE, interpretationStatusList);

        priorities.add(new ClinicalPriorityValue("URGENT", "Highest priority of all", 1, false));
        priorities.add(new ClinicalPriorityValue("HIGH", "Second highest priority of all", 2, false));
        priorities.add(new ClinicalPriorityValue("MEDIUM", "Intermediate priority", 3, false));
        priorities.add(new ClinicalPriorityValue("LOW", "Low priority", 4, false));
        priorities.add(new ClinicalPriorityValue("UNKNOWN", "Unknown priority. Treated as the lowest priority of all.", 5, true));

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

        clinicalConsentList.add(new ClinicalConsent("PRIMARY_FINDINGS", "Primary findings", ""));
        clinicalConsentList.add(new ClinicalConsent("SECONDARY_FINDINGS", "Secondary findings", ""));
        clinicalConsentList.add(new ClinicalConsent("CARRIER_FINDINGS", "Carrier findings", ""));
        clinicalConsentList.add(new ClinicalConsent("RESEARCH_FINDINGS", "Research findings", ""));

        return new ClinicalAnalysisStudyConfiguration(status,
                new InterpretationStudyConfiguration(interpretationStatus, Collections.emptyList(), Collections.emptyMap()), priorities,
                flags, new ClinicalConsentConfiguration(clinicalConsentList));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisStudyConfiguration{");
        sb.append("status=").append(status);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", priorities=").append(priorities);
        sb.append(", flags=").append(flags);
        sb.append(", consent=").append(consent);
        sb.append('}');
        return sb.toString();
    }

    public Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> getStatus() {
        return status;
    }

    public ClinicalAnalysisStudyConfiguration setStatus(Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> status) {
        this.status = status;
        return this;
    }

    public InterpretationStudyConfiguration getInterpretation() {
        return interpretation;
    }

    public ClinicalAnalysisStudyConfiguration setInterpretation(InterpretationStudyConfiguration interpretation) {
        this.interpretation = interpretation;
        return this;
    }

    public List<ClinicalPriorityValue> getPriorities() {
        return priorities;
    }

    public ClinicalAnalysisStudyConfiguration setPriorities(List<ClinicalPriorityValue> priorities) {
        this.priorities = priorities;
        return this;
    }

    public Map<ClinicalAnalysis.Type, List<FlagValue>> getFlags() {
        return flags;
    }

    public ClinicalAnalysisStudyConfiguration setFlags(Map<ClinicalAnalysis.Type, List<FlagValue>> flags) {
        this.flags = flags;
        return this;
    }

    public ClinicalConsentConfiguration getConsent() {
        return consent;
    }

    public ClinicalAnalysisStudyConfiguration setConsent(ClinicalConsentConfiguration consent) {
        this.consent = consent;
        return this;
    }
}
