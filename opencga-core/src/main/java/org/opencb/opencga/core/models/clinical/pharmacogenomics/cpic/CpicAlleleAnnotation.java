package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

/**
 * Wrapper holding a single star allele name and its CPIC allele-level information.
 */
public class CpicAlleleAnnotation {

    private String allele;
    private CpicAlleleInfo alleleInfo;

    public CpicAlleleAnnotation() {
    }

    public CpicAlleleAnnotation(String allele, CpicAlleleInfo alleleInfo) {
        this.allele = allele;
        this.alleleInfo = alleleInfo;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicAlleleAnnotation{");
        sb.append("allele='").append(allele).append('\'');
        sb.append(", alleleInfo=").append(alleleInfo);
        sb.append('}');
        return sb.toString();
    }

    public String getAllele() {
        return allele;
    }

    public CpicAlleleAnnotation setAllele(String allele) {
        this.allele = allele;
        return this;
    }

    public CpicAlleleInfo getAlleleInfo() {
        return alleleInfo;
    }

    public CpicAlleleAnnotation setAlleleInfo(CpicAlleleInfo alleleInfo) {
        this.alleleInfo = alleleInfo;
        return this;
    }
}
