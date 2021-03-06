package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class AlignmentGeneCoverageStatsParams extends ToolParams {
    public static final String DESCRIPTION = "Gene coverage stats parameters for a given BAM file and a list of genes";

    private String bamFile;
    private List<String> genes;
    private String outdir;

    public AlignmentGeneCoverageStatsParams() {
    }

    public AlignmentGeneCoverageStatsParams(String bamFile, List<String> genes, String outdir) {
        this.bamFile = bamFile;
        this.genes = genes;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentGeneCoverageStatsParams{");
        sb.append("bamFile='").append(bamFile).append('\'');
        sb.append(", genes=").append(genes);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getBamFile() {
        return bamFile;
    }

    public AlignmentGeneCoverageStatsParams setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public List<String> getGenes() {
        return genes;
    }

    public AlignmentGeneCoverageStatsParams setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public AlignmentGeneCoverageStatsParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
