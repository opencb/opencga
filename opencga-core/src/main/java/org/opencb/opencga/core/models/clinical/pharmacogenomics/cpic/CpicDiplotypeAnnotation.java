package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

import java.util.List;

public class CpicDiplotypeAnnotation {
    private String gene;           // e.g. "CYP2C9"
    private String diplotype;      // e.g. "*1/*6"
    private CpicDiplotypeInfo diplotypeInfo;
    private List<CpicDrugRecommendation> recommendations;

    public CpicDiplotypeAnnotation() {
    }

    public CpicDiplotypeAnnotation(String gene, String diplotype, CpicDiplotypeInfo diplotypeInfo,
                                   List<CpicDrugRecommendation> recommendations) {
        this.gene = gene;
        this.diplotype = diplotype;
        this.diplotypeInfo = diplotypeInfo;
        this.recommendations = recommendations;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicDiplotypeAnnotation{");
        sb.append("gene='").append(gene).append('\'');
        sb.append(", diplotype='").append(diplotype).append('\'');
        sb.append(", diplotypeInfo=").append(diplotypeInfo);
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

    public List<CpicDrugRecommendation> getRecommendations() {
        return recommendations;
    }

    public CpicDiplotypeAnnotation setRecommendations(List<CpicDrugRecommendation> recommendations) {
        this.recommendations = recommendations;
        return this;
    }
}
