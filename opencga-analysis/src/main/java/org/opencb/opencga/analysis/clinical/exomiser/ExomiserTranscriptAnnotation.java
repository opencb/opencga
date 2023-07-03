package org.opencb.opencga.analysis.clinical.exomiser;

public class ExomiserTranscriptAnnotation {

    private String variantEffect;
    private String geneSymbol;
    private String accession;
    private String hgvsGenomic;
    private String hgvsCdna;
    private String hgvsProtein;
    private String rankType;
    private int rank;
    private int rankTotal;

    public ExomiserTranscriptAnnotation() {
    }

    public ExomiserTranscriptAnnotation(String variantEffect, String geneSymbol, String accession, String hgvsGenomic, String hgvsCdna,
                                        String hgvsProtein, String rankType, int rank, int rankTotal) {
        this.variantEffect = variantEffect;
        this.geneSymbol = geneSymbol;
        this.accession = accession;
        this.hgvsGenomic = hgvsGenomic;
        this.hgvsCdna = hgvsCdna;
        this.hgvsProtein = hgvsProtein;
        this.rankType = rankType;
        this.rank = rank;
        this.rankTotal = rankTotal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExomiserTranscriptAnnotation{");
        sb.append("variantEffect='").append(variantEffect).append('\'');
        sb.append(", geneSymbol='").append(geneSymbol).append('\'');
        sb.append(", accession='").append(accession).append('\'');
        sb.append(", hgvsGenomic='").append(hgvsGenomic).append('\'');
        sb.append(", hgvsCdna='").append(hgvsCdna).append('\'');
        sb.append(", hgvsProtein='").append(hgvsProtein).append('\'');
        sb.append(", rankType='").append(rankType).append('\'');
        sb.append(", rank='").append(rank).append('\'');
        sb.append(", rankTotal='").append(rankTotal).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getVariantEffect() {
        return variantEffect;
    }

    public ExomiserTranscriptAnnotation setVariantEffect(String variantEffect) {
        this.variantEffect = variantEffect;
        return this;
    }

    public String getGeneSymbol() {
        return geneSymbol;
    }

    public ExomiserTranscriptAnnotation setGeneSymbol(String geneSymbol) {
        this.geneSymbol = geneSymbol;
        return this;
    }

    public String getAccession() {
        return accession;
    }

    public ExomiserTranscriptAnnotation setAccession(String accession) {
        this.accession = accession;
        return this;
    }

    public String getHgvsGenomic() {
        return hgvsGenomic;
    }

    public ExomiserTranscriptAnnotation setHgvsGenomic(String hgvsGenomic) {
        this.hgvsGenomic = hgvsGenomic;
        return this;
    }

    public String getHgvsCdna() {
        return hgvsCdna;
    }

    public ExomiserTranscriptAnnotation setHgvsCdna(String hgvsCdna) {
        this.hgvsCdna = hgvsCdna;
        return this;
    }

    public String getHgvsProtein() {
        return hgvsProtein;
    }

    public ExomiserTranscriptAnnotation setHgvsProtein(String hgvsProtein) {
        this.hgvsProtein = hgvsProtein;
        return this;
    }

    public String getRankType() {
        return rankType;
    }

    public ExomiserTranscriptAnnotation setRankType(String rankType) {
        this.rankType = rankType;
        return this;
    }

    public int getRank() {
        return rank;
    }

    public ExomiserTranscriptAnnotation setRank(int rank) {
        this.rank = rank;
        return this;
    }

    public int getRankTotal() {
        return rankTotal;
    }

    public ExomiserTranscriptAnnotation setRankTotal(int rankTotal) {
        this.rankTotal = rankTotal;
        return this;
    }
}
