package org.opencb.opencga.core.models.variant.fastqc;

public class PerBaseSeqQuality {
    // #Base	Mean	Median	Lower Quartile	Upper Quartile	10th Percentile	90th Percentile

    private String base;
    private double mean;
    private double median;
    private double quartileLower;
    private double quartileUpper;
    private double percentile10th;
    private double percentile90th;

    public PerBaseSeqQuality() {
    }

    public PerBaseSeqQuality(String base, double mean, double median, double quartileLower, double quartileUpper, double percentile10th,
                             double percentile90th) {
        this.base = base;
        this.mean = mean;
        this.median = median;
        this.quartileLower = quartileLower;
        this.quartileUpper = quartileUpper;
        this.percentile10th = percentile10th;
        this.percentile90th = percentile90th;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PerBaseSeqQuality{");
        sb.append("base='").append(base).append('\'');
        sb.append(", mean=").append(mean);
        sb.append(", median=").append(median);
        sb.append(", quartileLower=").append(quartileLower);
        sb.append(", quartileUpper=").append(quartileUpper);
        sb.append(", percentile10th=").append(percentile10th);
        sb.append(", percentile90th=").append(percentile90th);
        sb.append('}');
        return sb.toString();
    }

    public String getBase() {
        return base;
    }

    public PerBaseSeqQuality setBase(String base) {
        this.base = base;
        return this;
    }

    public double getMean() {
        return mean;
    }

    public PerBaseSeqQuality setMean(double mean) {
        this.mean = mean;
        return this;
    }

    public double getMedian() {
        return median;
    }

    public PerBaseSeqQuality setMedian(double median) {
        this.median = median;
        return this;
    }

    public double getQuartileLower() {
        return quartileLower;
    }

    public PerBaseSeqQuality setQuartileLower(double quartileLower) {
        this.quartileLower = quartileLower;
        return this;
    }

    public double getQuartileUpper() {
        return quartileUpper;
    }

    public PerBaseSeqQuality setQuartileUpper(double quartileUpper) {
        this.quartileUpper = quartileUpper;
        return this;
    }

    public double getPercentile10th() {
        return percentile10th;
    }

    public PerBaseSeqQuality setPercentile10th(double percentile10th) {
        this.percentile10th = percentile10th;
        return this;
    }

    public double getPercentile90th() {
        return percentile90th;
    }

    public PerBaseSeqQuality setPercentile90th(double percentile90th) {
        this.percentile90th = percentile90th;
        return this;
    }
}
