package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class SamtoolsWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Samtoolstools parameters";

    private String command;          // Valid values: view, index, sort, stats
    private String inputFile;        // Input file
    private String outputFilename;   // Output filename
    private String referenceFile;
    private String readGroupFile;
    private String bedFile;
    private String refSeqFile;
    private String referenceNamesFile;
    private String targetRegionFile;
    private String readsNotSelectedFilename;
    private String outdir;
    private Map<String, String> samtoolsParams;

    public SamtoolsWrapperParams() {
    }

    public SamtoolsWrapperParams(String command, String inputFile, String outputFilename, String referenceFile, String readGroupFile,
                                 String bedFile, String refSeqFile, String referenceNamesFile, String targetRegionFile,
                                 String readsNotSelectedFilename, String outdir, Map<String, String> samtoolsParams) {
        this.command = command;
        this.inputFile = inputFile;
        this.outputFilename = outputFilename;
        this.referenceFile = referenceFile;
        this.readGroupFile = readGroupFile;
        this.bedFile = bedFile;
        this.refSeqFile = refSeqFile;
        this.referenceNamesFile = referenceNamesFile;
        this.targetRegionFile = targetRegionFile;
        this.readsNotSelectedFilename = readsNotSelectedFilename;
        this.outdir = outdir;
        this.samtoolsParams = samtoolsParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SamtoolsWrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", inputFile='").append(inputFile).append('\'');
        sb.append(", outputFilename='").append(outputFilename).append('\'');
        sb.append(", referenceFile='").append(referenceFile).append('\'');
        sb.append(", readGroupFile='").append(readGroupFile).append('\'');
        sb.append(", bedFile='").append(bedFile).append('\'');
        sb.append(", refSeqFile='").append(refSeqFile).append('\'');
        sb.append(", referenceNamesFile='").append(referenceNamesFile).append('\'');
        sb.append(", targetRegionFile='").append(targetRegionFile).append('\'');
        sb.append(", readsNotSelectedFilename='").append(readsNotSelectedFilename).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", samtoolsParams=").append(samtoolsParams);
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public SamtoolsWrapperParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getInputFile() {
        return inputFile;
    }

    public SamtoolsWrapperParams setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }

    public String getOutputFilename() {
        return outputFilename;
    }

    public SamtoolsWrapperParams setOutputFilename(String outputFilename) {
        this.outputFilename = outputFilename;
        return this;
    }

    public String getReferenceFile() {
        return referenceFile;
    }

    public SamtoolsWrapperParams setReferenceFile(String referenceFile) {
        this.referenceFile = referenceFile;
        return this;
    }

    public String getReadGroupFile() {
        return readGroupFile;
    }

    public SamtoolsWrapperParams setReadGroupFile(String readGroupFile) {
        this.readGroupFile = readGroupFile;
        return this;
    }

    public String getBedFile() {
        return bedFile;
    }

    public SamtoolsWrapperParams setBedFile(String bedFile) {
        this.bedFile = bedFile;
        return this;
    }

    public String getRefSeqFile() {
        return refSeqFile;
    }

    public SamtoolsWrapperParams setRefSeqFile(String refSeqFile) {
        this.refSeqFile = refSeqFile;
        return this;
    }

    public String getReferenceNamesFile() {
        return referenceNamesFile;
    }

    public SamtoolsWrapperParams setReferenceNamesFile(String referenceNamesFile) {
        this.referenceNamesFile = referenceNamesFile;
        return this;
    }

    public String getTargetRegionFile() {
        return targetRegionFile;
    }

    public SamtoolsWrapperParams setTargetRegionFile(String targetRegionFile) {
        this.targetRegionFile = targetRegionFile;
        return this;
    }

    public String getReadsNotSelectedFilename() {
        return readsNotSelectedFilename;
    }

    public SamtoolsWrapperParams setReadsNotSelectedFilename(String readsNotSelectedFilename) {
        this.readsNotSelectedFilename = readsNotSelectedFilename;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public SamtoolsWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getSamtoolsParams() {
        return samtoolsParams;
    }

    public SamtoolsWrapperParams setSamtoolsParams(Map<String, String> samtoolsParams) {
        this.samtoolsParams = samtoolsParams;
        return this;
    }
}
