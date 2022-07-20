package org.opencb.opencga.core.models.study.configuration;

import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalStatusValue;

import java.util.List;
import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class InterpretationStudyConfiguration {

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> status;
    @DataField(description = ParamConstants.INTERPRETATION_STUDY_CONFIGURATION_VARIANT_CALLERS_DESCRIPTION)
    private List<InterpretationVariantCallerConfiguration> variantCallers;
    @DataField(description = ParamConstants.INTERPRETATION_STUDY_CONFIGURATION_DEFAULT_FILTER_DESCRIPTION)
    private Map<String, Object> defaultFilter;

    public InterpretationStudyConfiguration() {
    }

    public InterpretationStudyConfiguration(Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> status,
                                            List<InterpretationVariantCallerConfiguration> variantCallers,
                                            Map<String, Object> defaultFilter) {
        this.status = status;
        this.variantCallers = variantCallers;
        this.defaultFilter = defaultFilter;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationStudyConfiguration{");
        sb.append("status=").append(status);
        sb.append(", variantCallers=").append(variantCallers);
        sb.append(", defaultFilter=").append(defaultFilter);
        sb.append('}');
        return sb.toString();
    }

    public Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> getStatus() {
        return status;
    }

    public InterpretationStudyConfiguration setStatus(Map<ClinicalAnalysis.Type, List<ClinicalStatusValue>> status) {
        this.status = status;
        return this;
    }

    public List<InterpretationVariantCallerConfiguration> getVariantCallers() {
        return variantCallers;
    }

    public InterpretationStudyConfiguration setVariantCallers(List<InterpretationVariantCallerConfiguration> variantCallers) {
        this.variantCallers = variantCallers;
        return this;
    }

    public Map<String, Object> getDefaultFilter() {
        return defaultFilter;
    }

    public InterpretationStudyConfiguration setDefaultFilter(Map<String, Object> defaultFilter) {
        this.defaultFilter = defaultFilter;
        return this;
    }
}
