package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class MendelianErrorAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Mendelian error analysis params";
    private String family;
    private String individual;
    private String sample;
    private String outdir;

    public MendelianErrorAnalysisParams() {
    }

    public MendelianErrorAnalysisParams(String family, String individual, String sample, String outdir) {
        this.family = family;
        this.individual = individual;
        this.sample = sample;
        this.outdir = outdir;
    }

    public String getFamily() {
        return family;
    }

    public MendelianErrorAnalysisParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getIndividual() {
        return individual;
    }

    public MendelianErrorAnalysisParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public MendelianErrorAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public MendelianErrorAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
