package org.opencb.opencga.core.models.wrapper.deseq2;

public class DESeq2Output {

    private String basename;
    private boolean pcaPlot;
    private boolean maPlot;
    private boolean transformedCounts;

    public DESeq2Output() {
    }

    public DESeq2Output(String basename, boolean pcaPlot, boolean maPlot, boolean transformedCounts) {
        this.basename = basename;
        this.pcaPlot = pcaPlot;
        this.maPlot = maPlot;
        this.transformedCounts = transformedCounts;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DESeq2Output{");
        sb.append("basename='").append(basename).append('\'');
        sb.append(", pcaPlot=").append(pcaPlot);
        sb.append(", maPlot=").append(maPlot);
        sb.append(", transformedCounts=").append(transformedCounts);
        sb.append('}');
        return sb.toString();
    }

    public String getBasename() {
        return basename;
    }

    public DESeq2Output setBasename(String basename) {
        this.basename = basename;
        return this;
    }

    public boolean isPcaPlot() {
        return pcaPlot;
    }

    public DESeq2Output setPcaPlot(boolean pcaPlot) {
        this.pcaPlot = pcaPlot;
        return this;
    }

    public boolean isMaPlot() {
        return maPlot;
    }

    public DESeq2Output setMaPlot(boolean maPlot) {
        this.maPlot = maPlot;
        return this;
    }

    public boolean isTransformedCounts() {
        return transformedCounts;
    }

    public DESeq2Output setTransformedCounts(boolean transformedCounts) {
        this.transformedCounts = transformedCounts;
        return this;
    }
}
