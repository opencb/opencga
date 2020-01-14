package org.opencb.opencga.analysis.variant.genes.knockout;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class GeneKnockoutAnalysisParams extends ToolParams {

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

    public List<String> getSample() {
        return sample;
    }

    public GeneKnockoutAnalysisParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public List<String> getGene() {
        return gene;
    }

    public GeneKnockoutAnalysisParams setGene(List<String> gene) {
        this.gene = gene;
        return this;
    }

    public List<String> getPanel() {
        return panel;
    }

    public GeneKnockoutAnalysisParams setPanel(List<String> panel) {
        this.panel = panel;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public GeneKnockoutAnalysisParams setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getConsequenceType() {
        return consequenceType;
    }

    public GeneKnockoutAnalysisParams setConsequenceType(String consequenceType) {
        this.consequenceType = consequenceType;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public GeneKnockoutAnalysisParams setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public GeneKnockoutAnalysisParams setQual(String qual) {
        this.qual = qual;
        return this;
    }
}
