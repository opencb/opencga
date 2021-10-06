package org.opencb.opencga.core.models.study.configuration;

import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.StatusValue;

import java.util.List;
import java.util.Map;

public class InterpretationStudyConfiguration {

    private Map<ClinicalAnalysis.Type, List<StatusValue>> status;
    private List<InterpretationVariantCallerConfiguration> variantCallers;

    public InterpretationStudyConfiguration() {
    }

    public InterpretationStudyConfiguration(Map<ClinicalAnalysis.Type, List<StatusValue>> status,
                                            List<InterpretationVariantCallerConfiguration> variantCallers) {
        this.status = status;
        this.variantCallers = variantCallers;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationStudyConfiguration{");
        sb.append("status=").append(status);
        sb.append(", variantCallers=").append(variantCallers);
        sb.append('}');
        return sb.toString();
    }

    public Map<ClinicalAnalysis.Type, List<StatusValue>> getStatus() {
        return status;
    }

    public InterpretationStudyConfiguration setStatus(Map<ClinicalAnalysis.Type, List<StatusValue>> status) {
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
}
