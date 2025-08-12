package org.opencb.opencga.core.models.wrapper.deseq2;

public class DESeq2AnalysisContrast {

    private String factorName;
    private String numeratorLevel;
    private String denominatorLevel;

    public DESeq2AnalysisContrast() {
    }

    public DESeq2AnalysisContrast(String factorName, String numeratorLevel, String denominatorLevel) {
        this.factorName = factorName;
        this.numeratorLevel = numeratorLevel;
        this.denominatorLevel = denominatorLevel;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DESeq2AnalysisContrast{");
        sb.append("factorName='").append(factorName).append('\'');
        sb.append(", numeratorLevel='").append(numeratorLevel).append('\'');
        sb.append(", denominatorLevel='").append(denominatorLevel).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFactorName() {
        return factorName;
    }

    public DESeq2AnalysisContrast setFactorName(String factorName) {
        this.factorName = factorName;
        return this;
    }

    public String getNumeratorLevel() {
        return numeratorLevel;
    }

    public DESeq2AnalysisContrast setNumeratorLevel(String numeratorLevel) {
        this.numeratorLevel = numeratorLevel;
        return this;
    }

    public String getDenominatorLevel() {
        return denominatorLevel;
    }

    public DESeq2AnalysisContrast setDenominatorLevel(String denominatorLevel) {
        this.denominatorLevel = denominatorLevel;
        return this;
    }
}
