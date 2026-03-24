package org.opencb.opencga.core.models.clinical.pharmacogenomics;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class OpenArrayPharmacogenomicsAnalysisParams extends ToolParams {

    public static final String DESCRIPTION = "OpenArray pharmacogenomics analysis params";

    @DataField(id = "snvFile", description = "Catalog path to SNV genotyping file (ThermoFisher export)", required = true)
    private String snvFile;

    @DataField(id = "cnvFile", description = "Catalog path to CNV results file")
    private String cnvFile;

    @DataField(id = "translationFile", description = "Catalog path to translation table file", required = true)
    private String translationFile;

    @DataField(id = "renameFile", description = "Catalog path to allele rename file (HGVS nomenclature)")
    private String renameFile;

    @DataField(id = "compareToFile", description = "Catalog path to TrueMark detailed results file for comparison benchmark")
    private String compareToFile;

    @DataField(id = "annotate", description = "Run CPIC annotation on results")
    private Boolean annotate;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public OpenArrayPharmacogenomicsAnalysisParams() {
    }

    public OpenArrayPharmacogenomicsAnalysisParams(String snvFile, String cnvFile, String translationFile,
                                                   String renameFile, String compareToFile, Boolean annotate,
                                                   String outdir) {
        this.snvFile = snvFile;
        this.cnvFile = cnvFile;
        this.translationFile = translationFile;
        this.renameFile = renameFile;
        this.compareToFile = compareToFile;
        this.annotate = annotate;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpenArrayPharmacogenomicsAnalysisParams{");
        sb.append("snvFile='").append(snvFile).append('\'');
        sb.append(", cnvFile='").append(cnvFile).append('\'');
        sb.append(", translationFile='").append(translationFile).append('\'');
        sb.append(", renameFile='").append(renameFile).append('\'');
        sb.append(", compareToFile='").append(compareToFile).append('\'');
        sb.append(", annotate=").append(annotate);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSnvFile() { return snvFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setSnvFile(String snvFile) { this.snvFile = snvFile; return this; }

    public String getCnvFile() { return cnvFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setCnvFile(String cnvFile) { this.cnvFile = cnvFile; return this; }

    public String getTranslationFile() { return translationFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setTranslationFile(String translationFile) { this.translationFile = translationFile; return this; }

    public String getRenameFile() { return renameFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setRenameFile(String renameFile) { this.renameFile = renameFile; return this; }

    public String getCompareToFile() { return compareToFile; }
    public OpenArrayPharmacogenomicsAnalysisParams setCompareToFile(String compareToFile) { this.compareToFile = compareToFile; return this; }

    public Boolean getAnnotate() { return annotate; }
    public OpenArrayPharmacogenomicsAnalysisParams setAnnotate(Boolean annotate) { this.annotate = annotate; return this; }

    public String getOutdir() { return outdir; }
    public OpenArrayPharmacogenomicsAnalysisParams setOutdir(String outdir) { this.outdir = outdir; return this; }
}
