package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.RVTESTS_COMMAND_DESCRIPTION;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class RvtestsWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "RvTests parameters. " + RVTESTS_COMMAND_DESCRIPTION;

    @DataField(description = ParamConstants.RVTESTS_WRAPPER_PARAMS_COMMAND_DESCRIPTION)
    private String command;
    @DataField(description = ParamConstants.RVTESTS_WRAPPER_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;
    @DataField(description = ParamConstants.RVTESTS_WRAPPER_PARAMS_RVTESTS_PARAMS_DESCRIPTION)
    private Map<String, String> rvtestsParams;

    public RvtestsWrapperParams() {
    }

    public RvtestsWrapperParams(String command, String outdir, Map<String, String> rvtestsParams) {
        this.command = command;
        this.outdir = outdir;
        this.rvtestsParams = rvtestsParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RvtestsWrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", rvtestsParams=").append(rvtestsParams);
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public RvtestsWrapperParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public RvtestsWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getRvtestsParams() {
        return rvtestsParams;
    }

    public RvtestsWrapperParams setRvtestsParams(Map<String, String> rvtestsParams) {
        this.rvtestsParams = rvtestsParams;
        return this;
    }
}
