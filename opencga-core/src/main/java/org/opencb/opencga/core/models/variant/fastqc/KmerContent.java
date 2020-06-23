package org.opencb.opencga.core.models.variant.fastqc;

public class KmerContent {
// #Sequence	Count	PValue	Obs/Exp Max	Max Obs/Exp Position

    private String sequence;
    private int count;
    private double pValue;
    private double obsExpMax;
    private String maxObsExpPosition;

    public KmerContent() {
    }

    public KmerContent(String sequence, int count, double pValue, double obsExpMax, String maxObsExpPosition) {
        this.sequence = sequence;
        this.count = count;
        this.pValue = pValue;
        this.obsExpMax = obsExpMax;
        this.maxObsExpPosition = maxObsExpPosition;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KmerContent{");
        sb.append("sequence='").append(sequence).append('\'');
        sb.append(", count=").append(count);
        sb.append(", pValue=").append(pValue);
        sb.append(", obsExpMax=").append(obsExpMax);
        sb.append(", maxObsExpPosition='").append(maxObsExpPosition).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSequence() {
        return sequence;
    }

    public KmerContent setSequence(String sequence) {
        this.sequence = sequence;
        return this;
    }

    public int getCount() {
        return count;
    }

    public KmerContent setCount(int count) {
        this.count = count;
        return this;
    }

    public double getpValue() {
        return pValue;
    }

    public KmerContent setpValue(double pValue) {
        this.pValue = pValue;
        return this;
    }

    public double getObsExpMax() {
        return obsExpMax;
    }

    public KmerContent setObsExpMax(double obsExpMax) {
        this.obsExpMax = obsExpMax;
        return this;
    }

    public String getMaxObsExpPosition() {
        return maxObsExpPosition;
    }

    public KmerContent setMaxObsExpPosition(String maxObsExpPosition) {
        this.maxObsExpPosition = maxObsExpPosition;
        return this;
    }
}
