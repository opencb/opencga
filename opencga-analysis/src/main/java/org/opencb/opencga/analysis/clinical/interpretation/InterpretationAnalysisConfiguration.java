package org.opencb.opencga.analysis.clinical.interpretation;

public class InterpretationAnalysisConfiguration {
    private int maxLowCoverage;
    private boolean includeLowCoverage;
    private boolean skipUntieredVariants;

    public int getMaxLowCoverage() {
        return maxLowCoverage;
    }

    public InterpretationAnalysisConfiguration setMaxLowCoverage(int maxLowCoverage) {
        this.maxLowCoverage = maxLowCoverage;
        return this;
    }

    public boolean isIncludeLowCoverage() {
        return includeLowCoverage;
    }

    public InterpretationAnalysisConfiguration setIncludeLowCoverage(boolean includeLowCoverage) {
        this.includeLowCoverage = includeLowCoverage;
        return this;
    }

    public boolean isSkipUntieredVariants() {
        return skipUntieredVariants;
    }

    public InterpretationAnalysisConfiguration setSkipUntieredVariants(boolean skipUntieredVariants) {
        this.skipUntieredVariants = skipUntieredVariants;
        return this;
    }
}
