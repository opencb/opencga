package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class DeeptoolsWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Deeptools parameters";

    private String command;     // Valid values: bamCoverage
    private String bamFile;        // BAM file
    private String outdir;
    private Map<String, String> deeptoolsParams;

    public DeeptoolsWrapperParams() {
    }

    public DeeptoolsWrapperParams(String command, String bamFile, String outdir, Map<String, String> deeptoolsParams) {
        this.command = command;
        this.bamFile = bamFile;
        this.outdir = outdir;
        this.deeptoolsParams = deeptoolsParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DeeptoolsWrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", bamFile='").append(bamFile).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", deeptoolsParams=").append(deeptoolsParams);
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public DeeptoolsWrapperParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getBamFile() {
        return bamFile;
    }

    public DeeptoolsWrapperParams setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public DeeptoolsWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getDeeptoolsParams() {
        return deeptoolsParams;
    }

    public DeeptoolsWrapperParams setDeeptoolsParams(Map<String, String> deeptoolsParams) {
        this.deeptoolsParams = deeptoolsParams;
        return this;
    }
}
