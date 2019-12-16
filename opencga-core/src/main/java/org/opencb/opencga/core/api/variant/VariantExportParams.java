package org.opencb.opencga.core.api.variant;

import org.opencb.commons.datastore.core.Query;

public class VariantExportParams extends VariantQueryParams {
    public static final String DESCRIPTION = "Variant export params";
    private String outdir;
    private String outputFileName;
    private String outputFormat;
    private boolean compress;
    private String variantsFile;

    public VariantExportParams() {
    }

    public VariantExportParams(Query query, String outdir, String outputFileName, String outputFormat,
                               boolean compress, String variantsFile) {
        super(query);
        this.outdir = outdir;
        this.outputFileName = outputFileName;
        this.outputFormat = outputFormat;
        this.compress = compress;
        this.variantsFile = variantsFile;
    }

    public String getOutdir() {
        return outdir;
    }

    public VariantExportParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public VariantExportParams setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
        return this;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public VariantExportParams setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
        return this;
    }

    public boolean isCompress() {
        return compress;
    }

    public VariantExportParams setCompress(boolean compress) {
        this.compress = compress;
        return this;
    }

    public void setVariantsFile(String variantsFile) {
        this.variantsFile = variantsFile;
    }

    public String getVariantsFile() {
        return variantsFile;
    }
}
