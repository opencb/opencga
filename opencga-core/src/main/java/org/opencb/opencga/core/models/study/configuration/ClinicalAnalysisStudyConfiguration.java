package org.opencb.opencga.core.models.study.configuration;

import org.opencb.opencga.core.models.clinical.ClinicalStatusValue;
import org.opencb.opencga.core.models.common.FlagValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClinicalAnalysisStudyConfiguration {

    private List<ClinicalStatusValue> status;
    private InterpretationStudyConfiguration interpretation;
    private List<ClinicalPriorityValue> priorities;
    private List<FlagValue> flags;
    @Deprecated
    private ClinicalConsentConfiguration consent;
    private List<ClinicalConsent> consents;
    private List<ClinicalTierConfiguration> tiers;
    private ClinicalReportConfiguration report;


    public ClinicalAnalysisStudyConfiguration() {
    }

    public ClinicalAnalysisStudyConfiguration(List<ClinicalStatusValue> status, InterpretationStudyConfiguration interpretation,
                                              List<ClinicalPriorityValue> priorities, List<FlagValue> flags,
                                              List<ClinicalConsent> consents, List<ClinicalTierConfiguration> tiers,
                                              ClinicalReportConfiguration report) {
        this.status = status;
        this.interpretation = interpretation;
        this.priorities = priorities;
        this.flags = flags;
        this.consents = consents;
        this.tiers = tiers;
        this.report = report;
    }

    public static ClinicalAnalysisStudyConfiguration defaultConfiguration() {
        List<ClinicalStatusValue> clinicalStatusValueList = new ArrayList<>(4);
        List<ClinicalStatusValue> interpretationStatusList = new ArrayList<>(3);
        List<ClinicalPriorityValue> priorities = new ArrayList<>(5);
        List<ClinicalConsent> clinicalConsentList = new ArrayList<>();

        clinicalStatusValueList.add(
                new ClinicalStatusValue("READY_FOR_INTERPRETATION", "The Clinical Analysis is ready for interpretations",
                        ClinicalStatusValue.ClinicalStatusType.NOT_STARTED)
        );
        clinicalStatusValueList.add(
                new ClinicalStatusValue("READY_FOR_REPORT", "The Interpretation is finished and it is to create the report",
                        ClinicalStatusValue.ClinicalStatusType.ACTIVE)
        );
        clinicalStatusValueList.add(
                new ClinicalStatusValue("DONE", "The Clinical Analysis is done", ClinicalStatusValue.ClinicalStatusType.DONE)
        );
        clinicalStatusValueList.add(
                new ClinicalStatusValue("CLOSED", "The Clinical Analysis is closed", ClinicalStatusValue.ClinicalStatusType.CLOSED)
        );
        clinicalStatusValueList.add(
                new ClinicalStatusValue("REJECTED", "The Clinical Analysis is rejected", ClinicalStatusValue.ClinicalStatusType.CLOSED)
        );
        clinicalStatusValueList.add(
                new ClinicalStatusValue("INCONCLUSIVE", "No conclusions can be extracted from the Clinical Analysis",
                        ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE)
        );

        interpretationStatusList.add(new ClinicalStatusValue("NOT_STARTED", "Interpretation not started", ClinicalStatusValue.ClinicalStatusType.NOT_STARTED));
        interpretationStatusList.add(new ClinicalStatusValue("IN_PROGRESS", "Interpretation in progress", ClinicalStatusValue.ClinicalStatusType.ACTIVE));
        interpretationStatusList.add(new ClinicalStatusValue("DONE", "Interpretation done", ClinicalStatusValue.ClinicalStatusType.DONE));
        interpretationStatusList.add(new ClinicalStatusValue("READY", "Interpretation ready", ClinicalStatusValue.ClinicalStatusType.CLOSED));
        interpretationStatusList.add(new ClinicalStatusValue("REJECTED", "Interpretation rejected", ClinicalStatusValue.ClinicalStatusType.CLOSED));
        interpretationStatusList.add(new ClinicalStatusValue("INCONCLUSIVE", "Interpretation inconclusive", ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE));

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

        clinicalConsentList.add(new ClinicalConsent("PRIMARY_FINDINGS", "Primary findings", ""));
        clinicalConsentList.add(new ClinicalConsent("SECONDARY_FINDINGS", "Secondary findings", ""));
        clinicalConsentList.add(new ClinicalConsent("CARRIER_FINDINGS", "Carrier findings", ""));
        clinicalConsentList.add(new ClinicalConsent("RESEARCH_FINDINGS", "Research findings", ""));

        return new ClinicalAnalysisStudyConfiguration(clinicalStatusValueList,
                new InterpretationStudyConfiguration(interpretationStatusList, Collections.emptyList(), Collections.emptyMap(),
                        Collections.emptyList()), priorities, flagValueList, clinicalConsentList, Collections.emptyList(),
                ClinicalReportConfiguration.defaultClinicalReportConfiguration());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisStudyConfiguration{");
        sb.append("status=").append(status);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", priorities=").append(priorities);
        sb.append(", flags=").append(flags);
        sb.append(", consents=").append(consents);
        sb.append(", tiers=").append(tiers);
        sb.append(", report=").append(report);
        sb.append('}');
        return sb.toString();
    }

    public List<ClinicalStatusValue> getStatus() {
        return status;
    }

    public ClinicalAnalysisStudyConfiguration setStatus(List<ClinicalStatusValue> status) {
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

    public List<FlagValue> getFlags() {
        return flags;
    }

    public ClinicalAnalysisStudyConfiguration setFlags(List<FlagValue> flags) {
        this.flags = flags;
        return this;
    }

    @Deprecated
    public ClinicalConsentConfiguration getConsent() {
        return new ClinicalConsentConfiguration(consents);
    }

    @Deprecated
    public ClinicalAnalysisStudyConfiguration setConsent(ClinicalConsentConfiguration consent) {
        this.consents = consent.getConsents();
        return this;
    }

    public List<ClinicalConsent> getConsents() {
        return consents;
    }

    public ClinicalAnalysisStudyConfiguration setConsents(List<ClinicalConsent> consents) {
        this.consents = consents;
        return this;
    }

    public List<ClinicalTierConfiguration> getTiers() {
        return tiers;
    }

    public ClinicalAnalysisStudyConfiguration setTiers(List<ClinicalTierConfiguration> tiers) {
        this.tiers = tiers;
        return this;
    }

    public ClinicalReportConfiguration getReport() {
        return report;
    }

    public ClinicalAnalysisStudyConfiguration setReport(ClinicalReportConfiguration report) {
        this.report = report;
        return this;
    }
}
