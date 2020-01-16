package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class KnockoutAnalysisParams extends ToolParams {

    public static final String DESCRIPTION = "Gene knockout analysis params";
    // Sample filter
    private List<String> sample;
    // family
    // phenotype
    // disorder
    // cohort
    // annotation

    // Gene filter
    private List<String> gene;
    private List<String> panel;
    private String biotype;
    // HPO

    // Variant filter
    private String consequenceType;
    private String filter;
    private String qual;


    private String outdir;

    public KnockoutAnalysisParams() {
    }

    public KnockoutAnalysisParams(List<String> sample, List<String> gene, List<String> panel, String biotype, String consequenceType,
                                  String filter, String qual, String outdir) {
        this.sample = sample;
        this.gene = gene;
        this.panel = panel;
        this.biotype = biotype;
        this.consequenceType = consequenceType;
        this.filter = filter;
        this.qual = qual;
        this.outdir = outdir;
    }

    public List<String> getSample() {
        return sample;
    }

    public KnockoutAnalysisParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public List<String> getGene() {
        return gene;
    }

    public KnockoutAnalysisParams setGene(List<String> gene) {
        this.gene = gene;
        return this;
    }

    public List<String> getPanel() {
        return panel;
    }

    public KnockoutAnalysisParams setPanel(List<String> panel) {
        this.panel = panel;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public KnockoutAnalysisParams setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getConsequenceType() {
        return consequenceType;
    }

    public KnockoutAnalysisParams setConsequenceType(String consequenceType) {
        this.consequenceType = consequenceType;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public KnockoutAnalysisParams setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public KnockoutAnalysisParams setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public KnockoutAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
