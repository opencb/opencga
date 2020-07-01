package org.opencb.opencga.core.models.sample;

import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.biodata.models.clinical.Comment;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.opencga.core.models.variant.HsMetricsReport;

import java.util.ArrayList;
import java.util.List;

public class SampleQualityControlMetrics {

    private String bamFileId;

    private List<SampleQcVariantStats> variantStats;
    private FastQc fastQc;
    private SamtoolsFlagstats samtoolsFlagstats;
    private HsMetricsReport hsMetricsReport;
    private List<GeneCoverageStats> geneCoverageStats;
    private Signature signature;

    List<String> fileIds;
    List<Comment> comments;

    public SampleQualityControlMetrics() {
        this(null, new ArrayList<>(), null, null, null, new ArrayList<>(), null, new ArrayList<>(), new ArrayList<>());
    }

    public SampleQualityControlMetrics(String bamFileId, List<SampleQcVariantStats> variantStats, FastQc fastQc,
                                       SamtoolsFlagstats samtoolsFlagstats, HsMetricsReport hsMetricsReport,
                                       List<GeneCoverageStats> geneCoverageStats, Signature signature, List<String> fileIds,
                                       List<Comment> comments) {
        this.bamFileId = bamFileId;
        this.variantStats = variantStats;
        this.fastQc = fastQc;
        this.samtoolsFlagstats = samtoolsFlagstats;
        this.hsMetricsReport = hsMetricsReport;
        this.geneCoverageStats = geneCoverageStats;
        this.signature = signature;
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
        sb.append(", hsMetricsReport=").append(hsMetricsReport);
        sb.append(", geneCoverageStats=").append(geneCoverageStats);
        sb.append(", signature=").append(signature);
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

    public HsMetricsReport getHsMetricsReport() {
        return hsMetricsReport;
    }

    public SampleQualityControlMetrics setHsMetricsReport(HsMetricsReport hsMetricsReport) {
        this.hsMetricsReport = hsMetricsReport;
        return this;
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public SampleQualityControlMetrics setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }

    public Signature getSignature() {
        return signature;
    }

    public SampleQualityControlMetrics setSignature(Signature signature) {
        this.signature = signature;
        return this;
    }

    public List<String> getFileIds() {
        return fileIds;
    }

    public SampleQualityControlMetrics setFileIds(List<String> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public SampleQualityControlMetrics setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }
}
