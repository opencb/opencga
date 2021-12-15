package org.opencb.opencga.core.models.sample;

import org.opencb.biodata.models.clinical.qc.GenomePlot;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.ArrayList;
import java.util.List;

public class SampleVariantQualityControlMetrics {

    @DataField(id = "SampleVariantQualityControlMetrics.variantStats", name = "variantStats",
            description = FieldConstants.SAMPLE_QUALITY_CONTROL_METRICS_VARIANT_STATS_DESCRIPTION)
    private List<SampleQcVariantStats> variantStats;

    @DataField(id = "SampleVariantQualityControlMetrics.signatures", name = "signatures",
            description = FieldConstants.SAMPLE_QUALITY_CONTROL_METRICS_SIGNATURES_DESCRIPTION)
    private List<Signature> signatures;

    @DataField(id = "SampleVariantQualityControlMetrics.genomePlot", name = "genomePlot",
            description = FieldConstants.SAMPLE_QUALITY_CONTROL_METRICS_GENOME_PLOT_DESCRIPTION)
    private GenomePlot genomePlot;

    @DataField(id = "SampleVariantQualityControlMetrics.files", name = "files",
            description = FieldConstants.SAMPLE_QUALITY_CONTROL_METRICS_FILES_DESCRIPTION)
    private List<String> files;

    public SampleVariantQualityControlMetrics() {
        this(new ArrayList<>(), new ArrayList<>(), null, new ArrayList<>());
    }

    public SampleVariantQualityControlMetrics(List<SampleQcVariantStats> variantStats, List<Signature> signatures,
                                              GenomePlot genomePlot, List<String> files) {
        this.variantStats = variantStats;
        this.signatures = signatures;
        this.genomePlot = genomePlot;
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleVariantQualityControlMetrics{");
        sb.append("variantStats=").append(variantStats);
        sb.append(", signatures=").append(signatures);
        sb.append(", genomePlot=").append(genomePlot);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public List<SampleQcVariantStats> getVariantStats() {
        return variantStats;
    }

    public SampleVariantQualityControlMetrics setVariantStats(List<SampleQcVariantStats> variantStats) {
        this.variantStats = variantStats;
        return this;
    }

    public List<Signature> getSignatures() {
        return signatures;
    }

    public SampleVariantQualityControlMetrics setSignatures(List<Signature> signatures) {
        this.signatures = signatures;
        return this;
    }

    public GenomePlot getGenomePlot() {
        return genomePlot;
    }

    public SampleVariantQualityControlMetrics setGenomePlot(GenomePlot genomePlot) {
        this.genomePlot = genomePlot;
        return this;
    }

    public List<String> getFiles() {
        return files;
    }

    public SampleVariantQualityControlMetrics setFiles(List<String> files) {
        this.files = files;
        return this;
    }
}
