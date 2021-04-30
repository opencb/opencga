package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class BwaWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "BWA parameters";

    private String command;       // Valid values: index or mem
    private String fastaFile;     //  Fasta file
    private String fastq1File;    // FastQ #1 file
    private String fastq2File;    // FastQ #2 file
    private String outdir;
    private Map<String, String> bwaParams;

    public BwaWrapperParams() {
    }

    public BwaWrapperParams(String command, String fastaFile, String fastq1File, String fastq2File, String outdir,
                            Map<String, String> bwaParams) {
        this.command = command;
        this.fastaFile = fastaFile;
        this.fastq1File = fastq1File;
        this.fastq2File = fastq2File;
        this.outdir = outdir;
        this.bwaParams = bwaParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BwaWrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", fastaFile='").append(fastaFile).append('\'');
        sb.append(", fastq1File='").append(fastq1File).append('\'');
        sb.append(", fastq2File='").append(fastq2File).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", bwaParams=").append(bwaParams);
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public BwaWrapperParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getFastaFile() {
        return fastaFile;
    }

    public BwaWrapperParams setFastaFile(String fastaFile) {
        this.fastaFile = fastaFile;
        return this;
    }

    public String getFastq1File() {
        return fastq1File;
    }

    public BwaWrapperParams setFastq1File(String fastq1File) {
        this.fastq1File = fastq1File;
        return this;
    }

    public String getFastq2File() {
        return fastq2File;
    }

    public BwaWrapperParams setFastq2File(String fastq2File) {
        this.fastq2File = fastq2File;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public BwaWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getBwaParams() {
        return bwaParams;
    }

    public BwaWrapperParams setBwaParams(Map<String, String> bwaParams) {
        this.bwaParams = bwaParams;
        return this;
    }
}
