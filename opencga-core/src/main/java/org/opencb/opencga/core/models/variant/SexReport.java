package org.opencb.opencga.core.models.variant;

public class SexReport {

    // Sample ID
    private String sampleId;

    // Reported values
    private String reportedSex;
    private String reportedKaryotypicSex;

    // Ratio: X-chrom / autosoma-chroms
    private double ratioX;

    // Ratio: Y-chrom / autosoma-chroms
    private double ratioY;

    // Inferred karyotypic sex
    private String inferredKaryotypicSex;

    public SexReport() {
    }

    public SexReport(String reportedSex, String reportedKaryotypicSex, double ratioX, double ratioY, String inferredKaryotypicSex) {
        this.reportedSex = reportedSex;
        this.reportedKaryotypicSex = reportedKaryotypicSex;
        this.ratioX = ratioX;
        this.ratioY = ratioY;
        this.inferredKaryotypicSex = inferredKaryotypicSex;
    }

    public String getReportedSex() {
        return reportedSex;
    }

    public SexReport setReportedSex(String reportedSex) {
        this.reportedSex = reportedSex;
        return this;
    }

    public String getReportedKaryotypicSex() {
        return reportedKaryotypicSex;
    }

    public SexReport setReportedKaryotypicSex(String reportedKaryotypicSex) {
        this.reportedKaryotypicSex = reportedKaryotypicSex;
        return this;
    }

    public double getRatioX() {
        return ratioX;
    }

    public SexReport setRatioX(double ratioX) {
        this.ratioX = ratioX;
        return this;
    }

    public double getRatioY() {
        return ratioY;
    }

    public SexReport setRatioY(double ratioY) {
        this.ratioY = ratioY;
        return this;
    }

    public String getInferredKaryotypicSex() {
        return inferredKaryotypicSex;
    }

    public SexReport setInferredKaryotypicSex(String inferredKaryotypicSex) {
        this.inferredKaryotypicSex = inferredKaryotypicSex;
        return this;
    }
}
