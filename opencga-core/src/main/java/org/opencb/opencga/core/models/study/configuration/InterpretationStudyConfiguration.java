package org.opencb.opencga.core.models.study.configuration;

import org.opencb.opencga.core.models.clinical.ClinicalStatusValue;

import java.util.List;
import java.util.Map;

public class InterpretationStudyConfiguration {

    private List<ClinicalStatusValue> status;
    private List<InterpretationVariantCallerConfiguration> variantCallers;
    @Deprecated
    private Map<String, Object> defaultFilter;
    private List<VariantInclusionQueryConfiguration> inclusion;

    public InterpretationStudyConfiguration() {
    }

    public InterpretationStudyConfiguration(List<ClinicalStatusValue> status, List<InterpretationVariantCallerConfiguration> variantCallers,
                                            Map<String, Object> defaultFilter, List<VariantInclusionQueryConfiguration> inclusion) {
        this.status = status;
        this.variantCallers = variantCallers;
        this.defaultFilter = defaultFilter;
        this.inclusion = inclusion;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationStudyConfiguration{");
        sb.append("status=").append(status);
        sb.append(", variantCallers=").append(variantCallers);
        sb.append(", defaultFilter=").append(defaultFilter);
        sb.append(", inclusion=").append(inclusion);
        sb.append('}');
        return sb.toString();
    }

    public List<ClinicalStatusValue> getStatus() {
        return status;
    }

    public InterpretationStudyConfiguration setStatus(List<ClinicalStatusValue> status) {
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

    @Deprecated
    public Map<String, Object> getDefaultFilter() {
        return defaultFilter;
    }

    @Deprecated
    public InterpretationStudyConfiguration setDefaultFilter(Map<String, Object> defaultFilter) {
        this.defaultFilter = defaultFilter;
        return this;
    }

    public List<VariantInclusionQueryConfiguration> getInclusion() {
        return inclusion;
    }

    public InterpretationStudyConfiguration setInclusion(List<VariantInclusionQueryConfiguration> inclusion) {
        this.inclusion = inclusion;
        return this;
    }
}
