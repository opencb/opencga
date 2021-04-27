package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentHsMetricsParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment hybrid-selection (HS) metrics params";

    private String bamFile;
    private String bedFile;
//    private String refSeqFile;
    private String dictFile;
    private String outdir;

    public AlignmentHsMetricsParams() {
    }

    public AlignmentHsMetricsParams(String bamFile, String bedFile, String dictFile, String outdir) {
        this.bamFile = bamFile;
        this.bedFile = bedFile;
        this.dictFile = dictFile;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentHsMetricsParams{");
        sb.append("bamFile='").append(bamFile).append('\'');
        sb.append(", bedFile='").append(bedFile).append('\'');
        sb.append(", dictFile='").append(dictFile).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getBamFile() {
        return bamFile;
    }

    public AlignmentHsMetricsParams setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getBedFile() {
        return bedFile;
    }

    public AlignmentHsMetricsParams setBedFile(String bedFile) {
        this.bedFile = bedFile;
        return this;
    }

    public String getDictFile() {
        return dictFile;
    }

    public AlignmentHsMetricsParams setDictFile(String dictFile) {
        this.dictFile = dictFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public AlignmentHsMetricsParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
