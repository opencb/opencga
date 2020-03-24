package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class IBDRelatednessAnalysisExecutor extends OpenCgaToolExecutor {

    private String studyId;
    private List<String> sampleIds;
    private String minorAlleleFreq;

    public IBDRelatednessAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public IBDRelatednessAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public IBDRelatednessAnalysisExecutor setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public IBDRelatednessAnalysisExecutor setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }
}
