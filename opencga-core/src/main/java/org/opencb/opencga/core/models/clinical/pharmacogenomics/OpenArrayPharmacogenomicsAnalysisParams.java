package org.opencb.opencga.core.models.clinical.pharmacogenomics;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class OpenArrayPharmacogenomicsAnalysisParams extends ToolParams {

    public static final String DESCRIPTION = "OpenArray pharmacogenomics analysis params";

    @DataField(id = "snvFile", description = "Catalog path to SNV genotyping file (ThermoFisher export)", required = true)
    private String snvFile;

    @DataField(id = "translationFile", description = "Catalog path to translation table file", required = true)
    private String translationFile;

    @DataField(id = "cnvFile", description = "Catalog path to CNV results file")
    private String cnvFile;

    @DataField(id = "renameFile", description = "Catalog path to allele rename file (HGVS nomenclature)")
    private String renameFile;

    @DataField(id = "compareTo", description = "Catalog path to TrueMark detailed results file for comparison benchmark")
    private String compareTo;

    @DataField(id = "annotate", description = "Run CPIC annotation on results")
    private Boolean annotate;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public OpenArrayPharmacogenomicsAnalysisParams() {
    }

    public OpenArrayPharmacogenomicsAnalysisParams(String snvFile, String translationFile, String cnvFile,
                                                   String renameFile, String compareTo, Boolean annotate,
                                                   String outdir) {
        this.snvFile = snvFile;
        this.translationFile = translationFile;
        this.cnvFile = cnvFile;
        this.renameFile = renameFile;
        this.compareTo = compareTo;
        this.annotate = annotate;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpenArrayPharmacogenomicsAnalysisParams{");
        sb.append("snvFile='").append(snvFile).append('\'');
        sb.append(", translationFile='").append(translationFile).append('\'');
        sb.append(", cnvFile='").append(cnvFile).append('\'');
        sb.append(", renameFile='").append(renameFile).append('\'');
        sb.append(", compareTo='").append(compareTo).append('\'');
        sb.append(", annotate=").append(annotate);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSnvFile() { return snvFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setSnvFile(String snvFile) { this.snvFile = snvFile; return this; }

    public String getTranslationFile() { return translationFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setTranslationFile(String translationFile) { this.translationFile = translationFile; return this; }

    public String getCnvFile() { return cnvFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setCnvFile(String cnvFile) { this.cnvFile = cnvFile; return this; }

    public String getRenameFile() { return renameFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setRenameFile(String renameFile) { this.renameFile = renameFile; return this; }

    public String getCompareTo() { return compareTo; }
    public OpenArrayPharmacogenomicsAnalysisParams setCompareTo(String compareTo) { this.compareTo = compareTo; return this; }

    public Boolean getAnnotate() { return annotate; }
    public OpenArrayPharmacogenomicsAnalysisParams setAnnotate(Boolean annotate) { this.annotate = annotate; return this; }

    public String getOutdir() { return outdir; }
    public OpenArrayPharmacogenomicsAnalysisParams setOutdir(String outdir) { this.outdir = outdir; return this; }
}
