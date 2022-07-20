package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.DEEPTOOLS_COMMAND_DESCRIPTION;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class DeeptoolsWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Deeptools parameters. " + DEEPTOOLS_COMMAND_DESCRIPTION;

    @DataField(description = ParamConstants.DEEPTOOLS_WRAPPER_PARAMS_COMMAND_DESCRIPTION)
    private String command;     // Valid values: bamCoverage
    @DataField(description = ParamConstants.DEEPTOOLS_WRAPPER_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;
    @DataField(description = ParamConstants.DEEPTOOLS_WRAPPER_PARAMS_DEEPTOOLS_PARAMS_DESCRIPTION)
    private Map<String, String> deeptoolsParams;

    public DeeptoolsWrapperParams() {
    }

    public DeeptoolsWrapperParams(String command, String outdir, Map<String, String> deeptoolsParams) {
        this.command = command;
        this.outdir = outdir;
        this.deeptoolsParams = deeptoolsParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DeeptoolsWrapperParams{");
        sb.append("command='").append(command).append('\'');
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
