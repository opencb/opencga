package org.opencb.opencga.core.models.study.configuration;

import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.StatusValue;

import java.util.List;
import java.util.Map;

public class InterpretationStudyConfiguration {

    private Map<ClinicalAnalysis.Type, List<StatusValue>> status;

    public InterpretationStudyConfiguration() {
    }

    public InterpretationStudyConfiguration(Map<ClinicalAnalysis.Type, List<StatusValue>> status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationStudyConfiguration{");
        sb.append("status=").append(status);
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
}
