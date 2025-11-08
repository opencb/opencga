package org.opencb.opencga.core.models.variant;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

@Deprecated
public class DeprecatedVariantWalkerParams extends VariantQueryParams {
    public static final String DESCRIPTION = "Variant walker params";

    @DataField(description = "Output file name")
    private String outputFileName;
    @DataField(description = "Format that will be used as input for the variant walker")
    private String inputFormat;
    @DataField(description = "Docker image to use")
    private String dockerImage;
    @DataField(description = "Command line to execute from the walker")
    private String commandLine;
    @DataField(description = ParamConstants.INCLUDE_DESCRIPTION)
    private String include;
    @DataField(description = ParamConstants.EXCLUDE_DESCRIPTION)
    private String exclude;

    public String getOutputFileName() {
        return outputFileName;
    }

    public DeprecatedVariantWalkerParams setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
        return this;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public DeprecatedVariantWalkerParams setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
        return this;
    }

    public String getDockerImage() {
        return dockerImage;
    }

    public DeprecatedVariantWalkerParams setDockerImage(String dockerImage) {
        this.dockerImage = dockerImage;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public DeprecatedVariantWalkerParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getInclude() {
        return include;
    }

    public DeprecatedVariantWalkerParams setInclude(String include) {
        this.include = include;
        return this;
    }

    public String getExclude() {
        return exclude;
    }

    public DeprecatedVariantWalkerParams setExclude(String exclude) {
        this.exclude = exclude;
        return this;
    }
}
