package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMAND_DESCRIPTION;

public class SamtoolsWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Samtools parameters. " + SAMTOOLS_COMMAND_DESCRIPTION;

    private String command;
    private String inputFile;
    private String outdir;
    private Map<String, String> samtoolsParams;

    public SamtoolsWrapperParams() {
    }

    public SamtoolsWrapperParams(String command, String inputFile, String outdir, Map<String, String> samtoolsParams) {
        this.command = command;
        this.inputFile = inputFile;
        this.outdir = outdir;
        this.samtoolsParams = samtoolsParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SamtoolsWrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", inputFile='").append(inputFile).append('\'');
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
