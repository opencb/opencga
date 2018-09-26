package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.alignment.RegionCoverage;

public class ReportedLowCoverage {
    private String geneName;
    private String chromosome;
    private int start;
    private int end;
    private double meanCoverage;
    private String id;
    private String type;

    public ReportedLowCoverage() {
    }

    public ReportedLowCoverage(RegionCoverage regionCoverage) {
        this.chromosome = regionCoverage.getChromosome();
        this.start = regionCoverage.getStart();
        this.end = regionCoverage.getEnd();

        float coverage = 0;
        for (float value: regionCoverage.getValues()) {
            coverage += value;
        }
        this.meanCoverage = coverage / regionCoverage.getValues().length;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ReportedLowCoverage{");
        sb.append("geneName='").append(geneName).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", meanCoverage=").append(meanCoverage);
        sb.append(", id='").append(id).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getGeneName() {
        return geneName;
    }

    public ReportedLowCoverage setGeneName(String geneName) {
        this.geneName = geneName;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public ReportedLowCoverage setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public ReportedLowCoverage setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public ReportedLowCoverage setEnd(int end) {
        this.end = end;
        return this;
    }

    public double getMeanCoverage() {
        return meanCoverage;
    }

    public ReportedLowCoverage setMeanCoverage(double meanCoverage) {
        this.meanCoverage = meanCoverage;
        return this;
    }

    public String getId() {
        return id;
    }

    public ReportedLowCoverage setId(String id) {
        this.id = id;
        return this;
    }

    public String getType() {
        return type;
    }

    public ReportedLowCoverage setType(String type) {
        this.type = type;
        return this;
    }
}
