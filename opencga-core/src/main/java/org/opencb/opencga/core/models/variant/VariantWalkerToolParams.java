package org.opencb.opencga.core.models.variant;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantWalkerToolParams extends VariantQueryParams {
    public static final String DESCRIPTION = "Variant walker params";


    @DataField(description = "Output file name")
    private String outputFileName;
    @DataField(description = "Format that will be used as input for the variant walker")
    private String inputFormat;
    @DataField(description = ParamConstants.INCLUDE_DESCRIPTION)
    private String include;
    @DataField(description = ParamConstants.EXCLUDE_DESCRIPTION)
    private String exclude;
    @DataField(description = "Command line to execute from the walker")
    private String commandLine;

    public VariantWalkerToolParams() {
    }

    public VariantWalkerToolParams(String outputFileName, String inputFormat, String include, String exclude) {
        this.outputFileName = outputFileName;
        this.inputFormat = inputFormat;
        this.include = include;
        this.exclude = exclude;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantWalkerParams2{");
        sb.append("outputFileName='").append(outputFileName).append('\'');
        sb.append(", inputFormat='").append(inputFormat).append('\'');
        sb.append(", include='").append(include).append('\'');
        sb.append(", exclude='").append(exclude).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public VariantWalkerToolParams setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
        return this;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public VariantWalkerToolParams setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
        return this;
    }

    public String getInclude() {
        return include;
    }

    public VariantWalkerToolParams setInclude(String include) {
        this.include = include;
        return this;
    }

    public String getExclude() {
        return exclude;
    }

    public VariantWalkerToolParams setExclude(String exclude) {
        this.exclude = exclude;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public VariantWalkerToolParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }
}
