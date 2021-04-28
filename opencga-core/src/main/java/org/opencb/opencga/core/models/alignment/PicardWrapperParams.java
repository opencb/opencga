package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class PicardWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Picard parameters.";

    private String command;
    private String bamFile;
    private String bedFile;
    private String baitIntervalsFile;
    private String targetIntervalsFile;
    private String dictFile;
    private String refSeqFile;
    private String outFilename;
    private String outdir;
    private Map<String, String> picardParams;

    public PicardWrapperParams() {
    }

    public PicardWrapperParams(String command, String bamFile, String bedFile, String baitIntervalsFile, String targetIntervalsFile,
                               String dictFile, String refSeqFile, String outFilename, String outdir, Map<String, String> picardParams) {
        this.command = command;
        this.bamFile = bamFile;
        this.bedFile = bedFile;
        this.baitIntervalsFile = baitIntervalsFile;
        this.targetIntervalsFile = targetIntervalsFile;
        this.dictFile = dictFile;
        this.refSeqFile = refSeqFile;
        this.outFilename = outFilename;
        this.outdir = outdir;
        this.picardParams = picardParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PicardWrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", bamFile='").append(bamFile).append('\'');
        sb.append(", bedFile='").append(bedFile).append('\'');
        sb.append(", baitIntervalsFile='").append(baitIntervalsFile).append('\'');
        sb.append(", targetIntervalsFile='").append(targetIntervalsFile).append('\'');
        sb.append(", dictFile='").append(dictFile).append('\'');
        sb.append(", refSeqFile='").append(refSeqFile).append('\'');
        sb.append(", outFilename='").append(outFilename).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", picardParams=").append(picardParams);
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public PicardWrapperParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getBamFile() {
        return bamFile;
    }

    public PicardWrapperParams setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getBedFile() {
        return bedFile;
    }

    public PicardWrapperParams setBedFile(String bedFile) {
        this.bedFile = bedFile;
        return this;
    }

    public String getBaitIntervalsFile() {
        return baitIntervalsFile;
    }

    public PicardWrapperParams setBaitIntervalsFile(String baitIntervalsFile) {
        this.baitIntervalsFile = baitIntervalsFile;
        return this;
    }

    public String getTargetIntervalsFile() {
        return targetIntervalsFile;
    }

    public PicardWrapperParams setTargetIntervalsFile(String targetIntervalsFile) {
        this.targetIntervalsFile = targetIntervalsFile;
        return this;
    }

    public String getDictFile() {
        return dictFile;
    }

    public PicardWrapperParams setDictFile(String dictFile) {
        this.dictFile = dictFile;
        return this;
    }

    public String getRefSeqFile() {
        return refSeqFile;
    }

    public PicardWrapperParams setRefSeqFile(String refSeqFile) {
        this.refSeqFile = refSeqFile;
        return this;
    }

    public String getOutFilename() {
        return outFilename;
    }

    public PicardWrapperParams setOutFilename(String outFilename) {
        this.outFilename = outFilename;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public PicardWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getPicardParams() {
        return picardParams;
    }

    public PicardWrapperParams setPicardParams(Map<String, String> picardParams) {
        this.picardParams = picardParams;
        return this;
    }
}
