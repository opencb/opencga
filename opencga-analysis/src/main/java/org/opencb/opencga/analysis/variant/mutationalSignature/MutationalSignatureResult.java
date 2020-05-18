package org.opencb.opencga.analysis.variant.mutationalSignature;

import java.util.Map;

public class MutationalSignatureResult {
    Map<String, Integer> counts;
    Map<String, Double> coeffs;
    double rss;
    private String summaryImg;

    public MutationalSignatureResult() {
    }

    public MutationalSignatureResult(Map<String, Integer> counts, Map<String, Double> coeffs, double rss, String summaryImg) {
        this.counts = counts;
        this.coeffs = coeffs;
        this.rss = rss;
        this.summaryImg = summaryImg;
    }

    public Map<String, Integer> getCounts() {
        return counts;
    }

    public MutationalSignatureResult setCounts(Map<String, Integer> counts) {
        this.counts = counts;
        return this;
    }

    public Map<String, Double> getCoeffs() {
        return coeffs;
    }

    public MutationalSignatureResult setCoeffs(Map<String, Double> coeffs) {
        this.coeffs = coeffs;
        return this;
    }

    public double getRss() {
        return rss;
    }

    public MutationalSignatureResult setRss(double rss) {
        this.rss = rss;
        return this;
    }

    public String getSummaryImg() {
        return summaryImg;
    }

    public MutationalSignatureResult setSummaryImg(String summaryImg) {
        this.summaryImg = summaryImg;
        return this;
    }
}
