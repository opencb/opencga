package org.opencb.opencga.core.models.wrapper.deseq2;

public class DESeq2Analysis {

        private String designFormula;
        private int prefilterMinCount;
        private String testMethod;
        private DESeq2AnalysisContrast contrast;

    public DESeq2Analysis() {
        this.contrast = new DESeq2AnalysisContrast();
    }

    public DESeq2Analysis(String designFormula, int prefilterMinCount, String testMethod, DESeq2AnalysisContrast contrast) {
        this.designFormula = designFormula;
        this.prefilterMinCount = prefilterMinCount;
        this.testMethod = testMethod;
        this.contrast = contrast;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DESeq2Analysis{");
        sb.append("designFormula='").append(designFormula).append('\'');
        sb.append(", prefilterMinCount=").append(prefilterMinCount);
        sb.append(", testMethod='").append(testMethod).append('\'');
        sb.append(", contrast=").append(contrast);
        sb.append('}');
        return sb.toString();
    }

    public String getDesignFormula() {
        return designFormula;
    }

    public DESeq2Analysis setDesignFormula(String designFormula) {
        this.designFormula = designFormula;
        return this;
    }

    public int getPrefilterMinCount() {
        return prefilterMinCount;
    }

    public DESeq2Analysis setPrefilterMinCount(int prefilterMinCount) {
        this.prefilterMinCount = prefilterMinCount;
        return this;
    }

    public String getTestMethod() {
        return testMethod;
    }

    public DESeq2Analysis setTestMethod(String testMethod) {
        this.testMethod = testMethod;
        return this;
    }

    public DESeq2AnalysisContrast getContrast() {
        return contrast;
    }

    public DESeq2Analysis setContrast(DESeq2AnalysisContrast contrast) {
        this.contrast = contrast;
        return this;
    }
}
