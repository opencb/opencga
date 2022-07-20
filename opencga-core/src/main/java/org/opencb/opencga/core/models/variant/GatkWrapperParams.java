package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.GATK_COMMAND_DESCRIPTION;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class GatkWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Gatk parameters. " + GATK_COMMAND_DESCRIPTION;

    @DataField(description = ParamConstants.GATK_WRAPPER_PARAMS_COMMAND_DESCRIPTION)
    private String command;
    @DataField(description = ParamConstants.GATK_WRAPPER_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;
    @DataField(description = ParamConstants.GATK_WRAPPER_PARAMS_GATK_PARAMS_DESCRIPTION)
    private Map<String, String> gatkParams;

    public GatkWrapperParams() {
    }

    public GatkWrapperParams(String command, String outdir, Map<String, String> gatkParams) {
        this.command = command;
        this.outdir = outdir;
        this.gatkParams = gatkParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GatkWrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", gatkParams=").append(gatkParams);
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public GatkWrapperParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public GatkWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getGatkParams() {
        return gatkParams;
    }

    public GatkWrapperParams setGatkParams(Map<String, String> gatkParams) {
        this.gatkParams = gatkParams;
        return this;
    }
}
