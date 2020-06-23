package org.opencb.opencga.core.models.variant.fastqc;

public class Summary {

    private String basicStatistics;
    private String perBaseSeqQuality;
    private String perTileSeqQuality;
    private String perSeqQualityScores;
    private String perBaseSeqContent;
    private String perSeqGcContent;
    private String perBaseNContent;
    private String seqLengthDistribution;
    private String seqDuplicationLevels;
    private String overrepresentedSeqs;
    private String adapterContent;
    private String kmerContent;

    public Summary() {
    }

    public Summary(String basicStatistics, String perBaseSeqQuality, String perTileSeqQuality, String perSeqQualityScores,
                   String perBaseSeqContent, String perSeqGcContent, String perBaseNContent, String seqLengthDistribution,
                   String seqDuplicationLevels, String overrepresentedSeqs, String adapterContent, String kmerContent) {
        this.basicStatistics = basicStatistics;
        this.perBaseSeqQuality = perBaseSeqQuality;
        this.perTileSeqQuality = perTileSeqQuality;
        this.perSeqQualityScores = perSeqQualityScores;
        this.perBaseSeqContent = perBaseSeqContent;
        this.perSeqGcContent = perSeqGcContent;
        this.perBaseNContent = perBaseNContent;
        this.seqLengthDistribution = seqLengthDistribution;
        this.seqDuplicationLevels = seqDuplicationLevels;
        this.overrepresentedSeqs = overrepresentedSeqs;
        this.adapterContent = adapterContent;
        this.kmerContent = kmerContent;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Summary{");
        sb.append("basicStatistics='").append(basicStatistics).append('\'');
        sb.append(", perBaseSeqQuality='").append(perBaseSeqQuality).append('\'');
        sb.append(", perTileSeqQuality='").append(perTileSeqQuality).append('\'');
        sb.append(", perSeqQualityScores='").append(perSeqQualityScores).append('\'');
        sb.append(", perBaseSeqContent='").append(perBaseSeqContent).append('\'');
        sb.append(", perSeqGcContent='").append(perSeqGcContent).append('\'');
        sb.append(", perBaseNContent='").append(perBaseNContent).append('\'');
        sb.append(", seqLengthDistribution='").append(seqLengthDistribution).append('\'');
        sb.append(", seqDuplicationLevels='").append(seqDuplicationLevels).append('\'');
        sb.append(", overrepresentedSeqs='").append(overrepresentedSeqs).append('\'');
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

    public String getPerBaseSeqQuality() {
        return perBaseSeqQuality;
    }

    public Summary setPerBaseSeqQuality(String perBaseSeqQuality) {
        this.perBaseSeqQuality = perBaseSeqQuality;
        return this;
    }

    public String getPerTileSeqQuality() {
        return perTileSeqQuality;
    }

    public Summary setPerTileSeqQuality(String perTileSeqQuality) {
        this.perTileSeqQuality = perTileSeqQuality;
        return this;
    }

    public String getPerSeqQualityScores() {
        return perSeqQualityScores;
    }

    public Summary setPerSeqQualityScores(String perSeqQualityScores) {
        this.perSeqQualityScores = perSeqQualityScores;
        return this;
    }

    public String getPerBaseSeqContent() {
        return perBaseSeqContent;
    }

    public Summary setPerBaseSeqContent(String perBaseSeqContent) {
        this.perBaseSeqContent = perBaseSeqContent;
        return this;
    }

    public String getPerSeqGcContent() {
        return perSeqGcContent;
    }

    public Summary setPerSeqGcContent(String perSeqGcContent) {
        this.perSeqGcContent = perSeqGcContent;
        return this;
    }

    public String getPerBaseNContent() {
        return perBaseNContent;
    }

    public Summary setPerBaseNContent(String perBaseNContent) {
        this.perBaseNContent = perBaseNContent;
        return this;
    }

    public String getSeqLengthDistribution() {
        return seqLengthDistribution;
    }

    public Summary setSeqLengthDistribution(String seqLengthDistribution) {
        this.seqLengthDistribution = seqLengthDistribution;
        return this;
    }

    public String getSeqDuplicationLevels() {
        return seqDuplicationLevels;
    }

    public Summary setSeqDuplicationLevels(String seqDuplicationLevels) {
        this.seqDuplicationLevels = seqDuplicationLevels;
        return this;
    }

    public String getOverrepresentedSeqs() {
        return overrepresentedSeqs;
    }

    public Summary setOverrepresentedSeqs(String overrepresentedSeqs) {
        this.overrepresentedSeqs = overrepresentedSeqs;
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
