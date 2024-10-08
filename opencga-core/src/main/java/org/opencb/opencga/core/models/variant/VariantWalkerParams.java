package org.opencb.opencga.core.models.variant;

public class VariantWalkerParams extends VariantQueryParams {
    public static final String DESCRIPTION = "Variant walker params";
    private String outdir;
    private String outputFileName;
    private String fileFormat;
    private String dockerImage;
    private String commandLine;
    private String include;
    private String exclude;

    public String getOutdir() {
        return outdir;
    }

    public VariantWalkerParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public VariantWalkerParams setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
        return this;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public VariantWalkerParams setFileFormat(String fileFormat) {
        this.fileFormat = fileFormat;
        return this;
    }

    public String getDockerImage() {
        return dockerImage;
    }

    public VariantWalkerParams setDockerImage(String dockerImage) {
        this.dockerImage = dockerImage;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public VariantWalkerParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getInclude() {
        return include;
    }

    public VariantWalkerParams setInclude(String include) {
        this.include = include;
        return this;
    }

    public String getExclude() {
        return exclude;
    }

    public VariantWalkerParams setExclude(String exclude) {
        this.exclude = exclude;
        return this;
    }
}
