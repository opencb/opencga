package org.opencb.opencga.core.models.variant.fastqc;

public class Summary {
    private String basicStatistics;
    private String perBaseSequenceQuality;
    private String perSequenceQualityScores;
    private String perBaseSequenceContent;
    private String perSequenceGcContent;
    private String perBaseNContent;
    private String sequenceLengthDistribution;
    private String sequenceDuplicationLevels;
    private String overrepresentedSequences;
    private String adapterContent;
    private String kmerContent;

    public Summary() {
    }

    public Summary(String basicStatistics, String perBaseSequenceQuality, String perSequenceQualityScores,
                   String perBaseSequenceContent, String perSequenceGcContent, String perBaseNContent,
                   String sequenceLengthDistribution, String sequenceDuplicationLevels, String overrepresentedSequences,
                   String adapterContent, String kmerContent) {
        this.basicStatistics = basicStatistics;
        this.perBaseSequenceQuality = perBaseSequenceQuality;
        this.perSequenceQualityScores = perSequenceQualityScores;
        this.perBaseSequenceContent = perBaseSequenceContent;
        this.perSequenceGcContent = perSequenceGcContent;
        this.perBaseNContent = perBaseNContent;
        this.sequenceLengthDistribution = sequenceLengthDistribution;
        this.sequenceDuplicationLevels = sequenceDuplicationLevels;
        this.overrepresentedSequences = overrepresentedSequences;
        this.adapterContent = adapterContent;
        this.kmerContent = kmerContent;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Summary{");
        sb.append("basicStatistics='").append(basicStatistics).append('\'');
        sb.append(", perBaseSequenceQuality='").append(perBaseSequenceQuality).append('\'');
        sb.append(", perSequenceQualityScores='").append(perSequenceQualityScores).append('\'');
        sb.append(", perBaseSequenceContent='").append(perBaseSequenceContent).append('\'');
        sb.append(", perSequenceGcContent='").append(perSequenceGcContent).append('\'');
        sb.append(", perBaseNContent='").append(perBaseNContent).append('\'');
        sb.append(", sequenceLengthDistribution='").append(sequenceLengthDistribution).append('\'');
        sb.append(", sequenceDuplicationLevels='").append(sequenceDuplicationLevels).append('\'');
        sb.append(", overrepresentedSequences='").append(overrepresentedSequences).append('\'');
        sb.append(", adapterContent='").append(adapterContent).append('\'');
        sb.append(", kmerContent='").append(kmerContent).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getBasicStatistics() {
        return basicStatistics;
    }

    public Summary setBasicStatistics(String basicStatistics) {
        this.basicStatistics = basicStatistics;
        return this;
    }

    public String getPerBaseSequenceQuality() {
        return perBaseSequenceQuality;
    }

    public Summary setPerBaseSequenceQuality(String perBaseSequenceQuality) {
        this.perBaseSequenceQuality = perBaseSequenceQuality;
        return this;
    }

    public String getPerSequenceQualityScores() {
        return perSequenceQualityScores;
    }

    public Summary setPerSequenceQualityScores(String perSequenceQualityScores) {
        this.perSequenceQualityScores = perSequenceQualityScores;
        return this;
    }

    public String getPerBaseSequenceContent() {
        return perBaseSequenceContent;
    }

    public Summary setPerBaseSequenceContent(String perBaseSequenceContent) {
        this.perBaseSequenceContent = perBaseSequenceContent;
        return this;
    }

    public String getPerSequenceGcContent() {
        return perSequenceGcContent;
    }

    public Summary setPerSequenceGcContent(String perSequenceGcContent) {
        this.perSequenceGcContent = perSequenceGcContent;
        return this;
    }

    public String getPerBaseNContent() {
        return perBaseNContent;
    }

    public Summary setPerBaseNContent(String perBaseNContent) {
        this.perBaseNContent = perBaseNContent;
        return this;
    }

    public String getSequenceLengthDistribution() {
        return sequenceLengthDistribution;
    }

    public Summary setSequenceLengthDistribution(String sequenceLengthDistribution) {
        this.sequenceLengthDistribution = sequenceLengthDistribution;
        return this;
    }

    public String getSequenceDuplicationLevels() {
        return sequenceDuplicationLevels;
    }

    public Summary setSequenceDuplicationLevels(String sequenceDuplicationLevels) {
        this.sequenceDuplicationLevels = sequenceDuplicationLevels;
        return this;
    }

    public String getOverrepresentedSequences() {
        return overrepresentedSequences;
    }

    public Summary setOverrepresentedSequences(String overrepresentedSequences) {
        this.overrepresentedSequences = overrepresentedSequences;
        return this;
    }

    public String getAdapterContent() {
        return adapterContent;
    }

    public Summary setAdapterContent(String adapterContent) {
        this.adapterContent = adapterContent;
        return this;
    }

    public String getKmerContent() {
        return kmerContent;
    }

    public Summary setKmerContent(String kmerContent) {
        this.kmerContent = kmerContent;
        return this;
    }
}
