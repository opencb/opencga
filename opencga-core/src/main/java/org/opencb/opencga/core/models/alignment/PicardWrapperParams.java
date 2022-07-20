package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

import org.opencb.commons.annotations.DataField;

public class PicardWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Picard parameters. " + ParamConstants.PICARD_COMMAND_DESCRIPTION;

    @DataField(description = ParamConstants.PICARD_WRAPPER_PARAMS_COMMAND_DESCRIPTION)
    private String command;
    @DataField(description = ParamConstants.PICARD_WRAPPER_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;
    @DataField(description = ParamConstants.PICARD_WRAPPER_PARAMS_PICARD_PARAMS_DESCRIPTION)
    private Map<String, String> picardParams;

    public PicardWrapperParams() {
    }

    public PicardWrapperParams(String command, String outdir, Map<String, String> picardParams) {
        this.command = command;
        this.outdir = outdir;
        this.picardParams = picardParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PicardWrapperParams{");
        sb.append("command='").append(command).append('\'');
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
