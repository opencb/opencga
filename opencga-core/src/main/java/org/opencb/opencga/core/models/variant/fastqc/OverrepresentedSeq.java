package org.opencb.opencga.core.models.variant.fastqc;

public class OverrepresentedSeq {
// #Sequence	Count	Percentage	Possible Source

    private String sequence;
    private int count;
    private double percentage;
    private String possibleSource;

    public OverrepresentedSeq() {
    }

    public OverrepresentedSeq(String sequence, int count, double percentage, String possibleSource) {
        this.sequence = sequence;
        this.count = count;
        this.percentage = percentage;
        this.possibleSource = possibleSource;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OverrepresentedSeq{");
        sb.append("sequence='").append(sequence).append('\'');
        sb.append(", count=").append(count);
        sb.append(", percentage=").append(percentage);
        sb.append(", possibleSource='").append(possibleSource).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSequence() {
        return sequence;
    }

    public OverrepresentedSeq setSequence(String sequence) {
        this.sequence = sequence;
        return this;
    }

    public int getCount() {
        return count;
    }

    public OverrepresentedSeq setCount(int count) {
        this.count = count;
        return this;
    }

    public double getPercentage() {
        return percentage;
    }

    public OverrepresentedSeq setPercentage(double percentage) {
        this.percentage = percentage;
        return this;
    }

    public String getPossibleSource() {
        return possibleSource;
    }

    public OverrepresentedSeq setPossibleSource(String possibleSource) {
        this.possibleSource = possibleSource;
        return this;
    }
}
