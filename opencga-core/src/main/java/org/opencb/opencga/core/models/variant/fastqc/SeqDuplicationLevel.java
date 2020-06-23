package org.opencb.opencga.core.models.variant.fastqc;

public class SeqDuplicationLevel {
// #Duplication Level	Percentage of deduplicated	Percentage of total

    private String level;
    private Double percDeduplicated;
    private Double percTotal;

    public SeqDuplicationLevel() {
    }

    public SeqDuplicationLevel(String level, Double percDeduplicated, Double percTotal) {
        this.level = level;
        this.percDeduplicated = percDeduplicated;
        this.percTotal = percTotal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SeqDuplicationLevel{");
        sb.append("level='").append(level).append('\'');
        sb.append(", percDeduplicated=").append(percDeduplicated);
        sb.append(", percTotal=").append(percTotal);
        sb.append('}');
        return sb.toString();
    }

    public String getLevel() {
        return level;
    }

    public SeqDuplicationLevel setLevel(String level) {
        this.level = level;
        return this;
    }

    public Double getPercDeduplicated() {
        return percDeduplicated;
    }

    public SeqDuplicationLevel setPercDeduplicated(Double percDeduplicated) {
        this.percDeduplicated = percDeduplicated;
        return this;
    }

    public Double getPercTotal() {
        return percTotal;
    }

    public SeqDuplicationLevel setPercTotal(Double percTotal) {
        this.percTotal = percTotal;
        return this;
    }
}
