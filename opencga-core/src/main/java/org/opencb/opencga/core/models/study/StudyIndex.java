package org.opencb.opencga.core.models.study;

public class StudyIndex {

    private RecessiveGeneSummaryIndex recessiveGene;

    public StudyIndex() {
    }

    public StudyIndex(RecessiveGeneSummaryIndex recessiveGene) {
        this.recessiveGene = recessiveGene;
    }

    public static StudyIndex init() {
        return new StudyIndex(RecessiveGeneSummaryIndex.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyIndex{");
        sb.append("recessiveGene=").append(recessiveGene);
        sb.append('}');
        return sb.toString();
    }

    public RecessiveGeneSummaryIndex getRecessiveGene() {
        return recessiveGene;
    }

    public StudyIndex setRecessiveGene(RecessiveGeneSummaryIndex recessiveGene) {
        this.recessiveGene = recessiveGene;
        return this;
    }
}
