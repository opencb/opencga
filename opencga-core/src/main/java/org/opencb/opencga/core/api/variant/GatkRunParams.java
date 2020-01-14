package org.opencb.opencga.core.api.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class GatkRunParams extends ToolParams {
    public static final String DESCRIPTION = "gatk params";
    private String command;      // Valid value: HaplotypeCaller
    private String fastaFile;    // FASTA file
    private String bamFile;      // BAM file
    private String vcfFilename;  // VCF filename
    private String outdir;
    private Map<String, String> gatkParams;

    public GatkRunParams() {
    }

    public GatkRunParams(String command, String fastaFile, String bamFile, String vcfFilename, String outdir,
                         Map<String, String> gatkParams) {
        this.command = command;
        this.fastaFile = fastaFile;
        this.bamFile = bamFile;
        this.vcfFilename = vcfFilename;
        this.outdir = outdir;
        this.gatkParams = gatkParams;
    }

    public String getCommand() {
        return command;
    }

    public GatkRunParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getFastaFile() {
        return fastaFile;
    }

    public GatkRunParams setFastaFile(String fastaFile) {
        this.fastaFile = fastaFile;
        return this;
    }

    public String getBamFile() {
        return bamFile;
    }

    public GatkRunParams setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getVcfFilename() {
        return vcfFilename;
    }

    public GatkRunParams setVcfFilename(String vcfFilename) {
        this.vcfFilename = vcfFilename;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public GatkRunParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getGatkParams() {
        return gatkParams;
    }

    public GatkRunParams setGatkParams(Map<String, String> gatkParams) {
        this.gatkParams = gatkParams;
        return this;
    }
}
