package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

import java.util.List;

/**
 * CPIC annotation for a diplotype (gene + allele pair).
 * Aggregates diplotype-level info, per-allele details, and drug recommendations.
 */
public class CpicDiplotypeAnnotation {

    private String gene;                              // e.g. "CYP2C9"
    private String diplotype;                         // e.g. "*1/*6"
    private CpicDiplotypeInfo diplotypeInfo;          // from /diplotype endpoint
    private List<CpicAlleleAnnotation> alleles;       // from /allele endpoint, one per allele
    private List<CpicDrugRecommendation> recommendations; // flat list from /recommendation endpoint

    public CpicDiplotypeAnnotation() {
    }

    public CpicDiplotypeAnnotation(String gene, String diplotype, CpicDiplotypeInfo diplotypeInfo,
                                   List<CpicAlleleAnnotation> alleles, List<CpicDrugRecommendation> recommendations) {
        this.gene = gene;
        this.diplotype = diplotype;
        this.diplotypeInfo = diplotypeInfo;
        this.alleles = alleles;
        this.recommendations = recommendations;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicDiplotypeAnnotation{");
        sb.append("gene='").append(gene).append('\'');
        sb.append(", diplotype='").append(diplotype).append('\'');
        sb.append(", diplotypeInfo=").append(diplotypeInfo);
        sb.append(", alleles=").append(alleles);
        sb.append(", recommendations=").append(recommendations);
        sb.append('}');
        return sb.toString();
    }

    public String getGene() {
        return gene;
    }

    public CpicDiplotypeAnnotation setGene(String gene) {
        this.gene = gene;
        return this;
    }

    public String getDiplotype() {
        return diplotype;
    }

    public CpicDiplotypeAnnotation setDiplotype(String diplotype) {
        this.diplotype = diplotype;
        return this;
    }

    public CpicDiplotypeInfo getDiplotypeInfo() {
        return diplotypeInfo;
    }

    public CpicDiplotypeAnnotation setDiplotypeInfo(CpicDiplotypeInfo diplotypeInfo) {
        this.diplotypeInfo = diplotypeInfo;
        return this;
    }

    public List<CpicAlleleAnnotation> getAlleles() {
        return alleles;
    }

    public CpicDiplotypeAnnotation setAlleles(List<CpicAlleleAnnotation> alleles) {
        this.alleles = alleles;
        return this;
    }

    public List<CpicDrugRecommendation> getRecommendations() {
        return recommendations;
    }

    public CpicDiplotypeAnnotation setRecommendations(List<CpicDrugRecommendation> recommendations) {
        this.recommendations = recommendations;
        return this;
    }
}
