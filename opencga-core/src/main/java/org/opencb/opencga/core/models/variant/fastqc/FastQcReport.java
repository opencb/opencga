package org.opencb.opencga.core.models.variant.fastqc;

import java.util.*;

public class FastQcReport {

    private Summary summary;
    private Map<String, String> basicStats;
    private List<PerBaseSeqQuality> perBaseSeqQualities;
    private Map<Integer, Double> perSeqQualityScores;
    private List<PerTileSeqQuality> perTileSeqQuality;
    private List<PerBaseSeqContent> perBaseSeqContent;
    private double[] perSeqGcContent;
    private Map<String, Double> perBaseNContent;
    private Map<Integer, Double> seqLengthDistribution;
    private List<SeqDuplicationLevel> seqDuplicationLevels;
    private List<OverrepresentedSeq> overrepresentedSeqs;
    private List<AdapterContent> adapterContent;
    private List<KmerContent> kmerContent;

    public FastQcReport() {
        summary = new Summary();
        basicStats = new LinkedHashMap<>();
        perBaseSeqQualities = new LinkedList<>();
        perSeqQualityScores = new LinkedHashMap<>();
        perTileSeqQuality = new LinkedList<>();
        perBaseSeqContent = new LinkedList<>();
        perSeqGcContent = new double[101];
        perBaseNContent = new LinkedHashMap<>();
        seqLengthDistribution = new LinkedHashMap<>();
        seqDuplicationLevels = new LinkedList<>();
        overrepresentedSeqs = new LinkedList<>();
        adapterContent = new LinkedList<>();
        kmerContent = new LinkedList<>();
    }

    public FastQcReport(Summary summary, Map<String, String> basicStats, List<PerBaseSeqQuality> perBaseSeqQualities,
                        Map<Integer, Double> perSeqQualityScores, List<PerTileSeqQuality> perTileSeqQuality,
                        List<PerBaseSeqContent> perBaseSeqContent, double[] perSeqGcContent, Map<String, Double> perBaseNContent,
                        Map<Integer, Double> seqLengthDistribution, List<SeqDuplicationLevel> seqDuplicationLevels,
                        List<OverrepresentedSeq> overrepresentedSeqs, List<AdapterContent> adapterContent,
                        List<KmerContent> kmerContent) {
        this.summary = summary;
        this.basicStats = basicStats;
        this.perBaseSeqQualities = perBaseSeqQualities;
        this.perSeqQualityScores = perSeqQualityScores;
        this.perTileSeqQuality = perTileSeqQuality;
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
        final StringBuilder sb = new StringBuilder("FastQcReport{");
        sb.append("summary=").append(summary);
        sb.append(", basicStats=").append(basicStats);
        sb.append(", perBaseSeqQualities=").append(perBaseSeqQualities);
        sb.append(", perSeqQualityScores=").append(perSeqQualityScores);
        sb.append(", perTileSeqQuality=").append(perTileSeqQuality);
        sb.append(", perBaseSeqContent=").append(perBaseSeqContent);
        sb.append(", perSeqGcContent=").append(Arrays.toString(perSeqGcContent));
        sb.append(", perBaseNContent=").append(perBaseNContent);
        sb.append(", seqLengthDistribution=").append(seqLengthDistribution);
        sb.append(", seqDuplicationLevels=").append(seqDuplicationLevels);
        sb.append(", overrepresentedSeqs=").append(overrepresentedSeqs);
        sb.append(", adapterContent=").append(adapterContent);
        sb.append(", kmerContent=").append(kmerContent);
        sb.append('}');
        return sb.toString();
    }

    public Summary getSummary() {
        return summary;
    }

    public FastQcReport setSummary(Summary summary) {
        this.summary = summary;
        return this;
    }

    public Map<String, String> getBasicStats() {
        return basicStats;
    }

    public FastQcReport setBasicStats(Map<String, String> basicStats) {
        this.basicStats = basicStats;
        return this;
    }

    public List<PerBaseSeqQuality> getPerBaseSeqQualities() {
        return perBaseSeqQualities;
    }

    public FastQcReport setPerBaseSeqQualities(List<PerBaseSeqQuality> perBaseSeqQualities) {
        this.perBaseSeqQualities = perBaseSeqQualities;
        return this;
    }

    public Map<Integer, Double> getPerSeqQualityScores() {
        return perSeqQualityScores;
    }

    public FastQcReport setPerSeqQualityScores(Map<Integer, Double> perSeqQualityScores) {
        this.perSeqQualityScores = perSeqQualityScores;
        return this;
    }

    public List<PerTileSeqQuality> getPerTileSeqQualities() {
        return perTileSeqQuality;
    }

    public FastQcReport setPerTileSeqQuality(List<PerTileSeqQuality> perTileSeqQuality) {
        this.perTileSeqQuality = perTileSeqQuality;
        return this;
    }

    public List<PerBaseSeqContent> getPerBaseSeqContent() {
        return perBaseSeqContent;
    }

    public FastQcReport setPerBaseSeqContent(List<PerBaseSeqContent> perBaseSeqContent) {
        this.perBaseSeqContent = perBaseSeqContent;
        return this;
    }

    public double[] getPerSeqGcContent() {
        return perSeqGcContent;
    }

    public FastQcReport setPerSeqGcContent(double[] perSeqGcContent) {
        this.perSeqGcContent = perSeqGcContent;
        return this;
    }

    public Map<String, Double> getPerBaseNContent() {
        return perBaseNContent;
    }

    public FastQcReport setPerBaseNContent(Map<String, Double> perBaseNContent) {
        this.perBaseNContent = perBaseNContent;
        return this;
    }

    public Map<Integer, Double> getSeqLengthDistribution() {
        return seqLengthDistribution;
    }

    public FastQcReport setSeqLengthDistribution(Map<Integer, Double> seqLengthDistribution) {
        this.seqLengthDistribution = seqLengthDistribution;
        return this;
    }

    public List<SeqDuplicationLevel> getSeqDuplicationLevels() {
        return seqDuplicationLevels;
    }

    public FastQcReport setSeqDuplicationLevels(List<SeqDuplicationLevel> seqDuplicationLevels) {
        this.seqDuplicationLevels = seqDuplicationLevels;
        return this;
    }

    public List<OverrepresentedSeq> getOverrepresentedSeqs() {
        return overrepresentedSeqs;
    }

    public FastQcReport setOverrepresentedSeqs(List<OverrepresentedSeq> overrepresentedSeqs) {
        this.overrepresentedSeqs = overrepresentedSeqs;
        return this;
    }

    public List<AdapterContent> getAdapterContent() {
        return adapterContent;
    }

    public FastQcReport setAdapterContent(List<AdapterContent> adapterContent) {
        this.adapterContent = adapterContent;
        return this;
    }

    public List<KmerContent> getKmerContent() {
        return kmerContent;
    }

    public FastQcReport setKmerContent(List<KmerContent> kmerContent) {
        this.kmerContent = kmerContent;
        return this;
    }
}
