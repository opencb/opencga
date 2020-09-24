package org.opencb.opencga.core.models.sample;

import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.clinical.qc.Signature;

import java.util.ArrayList;
import java.util.List;

public class SampleQualityControlMetrics {

    private String bamFileId;

    private List<SampleQcVariantStats> variantStats;
    private FastQc fastQc;
    private SamtoolsFlagstats samtoolsFlagstats;
    private HsMetrics hsMetrics;
    private List<GeneCoverageStats> geneCoverageStats;
    private List<Signature> signatures;

    List<String> fileIds;
    List<ClinicalComment> comments;

    public SampleQualityControlMetrics() {
        this(null, new ArrayList<>(), null, null, null, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public SampleQualityControlMetrics(String bamFileId, List<SampleQcVariantStats> variantStats, FastQc fastQc,
                                       SamtoolsFlagstats samtoolsFlagstats, HsMetrics hsMetrics,
                                       List<GeneCoverageStats> geneCoverageStats, List<Signature> signatures, List<String> fileIds,
                                       List<ClinicalComment> comments) {
        this.bamFileId = bamFileId;
        this.variantStats = variantStats;
        this.fastQc = fastQc;
        this.samtoolsFlagstats = samtoolsFlagstats;
        this.hsMetrics = hsMetrics;
        this.geneCoverageStats = geneCoverageStats;
        this.signatures = signatures;
        this.fileIds = fileIds;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQualityControlMetrics{");
        sb.append("bamFileId='").append(bamFileId).append('\'');
        sb.append(", variantStats=").append(variantStats);
        sb.append(", fastQc=").append(fastQc);
        sb.append(", samtoolsFlagstats=").append(samtoolsFlagstats);
        sb.append(", hsMetrics=").append(hsMetrics);
        sb.append(", geneCoverageStats=").append(geneCoverageStats);
        sb.append(", signatures=").append(signatures);
        sb.append(", fileIds=").append(fileIds);
        sb.append(", comments=").append(comments);
        sb.append('}');
        return sb.toString();
    }

    public String getBamFileId() {
        return bamFileId;
    }

    public SampleQualityControlMetrics setBamFileId(String bamFileId) {
        this.bamFileId = bamFileId;
        return this;
    }

    public List<SampleQcVariantStats> getVariantStats() {
        return variantStats;
    }

    public SampleQualityControlMetrics setVariantStats(List<SampleQcVariantStats> variantStats) {
        this.variantStats = variantStats;
        return this;
    }

    public FastQc getFastQc() {
        return fastQc;
    }

    public SampleQualityControlMetrics setFastQc(FastQc fastQc) {
        this.fastQc = fastQc;
        return this;
    }

    public SamtoolsFlagstats getSamtoolsFlagstats() {
        return samtoolsFlagstats;
    }

    public SampleQualityControlMetrics setSamtoolsFlagstats(SamtoolsFlagstats samtoolsFlagstats) {
        this.samtoolsFlagstats = samtoolsFlagstats;
        return this;
    }

    public HsMetrics getHsMetrics() {
        return hsMetrics;
    }

    public SampleQualityControlMetrics setHsMetrics(HsMetrics hsMetrics) {
        this.hsMetrics = hsMetrics;
        return this;
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public SampleQualityControlMetrics setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }

    public List<Signature> getSignatures() {
        return signatures;
    }

    public SampleQualityControlMetrics setSignatures(List<Signature> signatures) {
        this.signatures = signatures;
        return this;
    }

    public List<String> getFileIds() {
        return fileIds;
    }

    public SampleQualityControlMetrics setFileIds(List<String> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public SampleQualityControlMetrics setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }
}
