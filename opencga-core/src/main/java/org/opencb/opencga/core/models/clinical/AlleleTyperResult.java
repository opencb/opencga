package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Complete pharmacogenomics result for a single sample.
 */
public class AlleleTyperResult {

    @JsonProperty("sampleId")
    private String sampleId;

    @JsonProperty("starAlleles")
    private List<StarAlleleResult> starAlleles;

    @JsonProperty("genotypes")
    private List<Genotype> genotypes;

    @JsonProperty("translation")
    private List<TranslationInfo> translation;

    public AlleleTyperResult() {
    }

    public AlleleTyperResult(String sampleId, List<StarAlleleResult> starAlleles,
                                  List<Genotype> genotypes, List<TranslationInfo> translation) {
        this.sampleId = sampleId;
        this.starAlleles = starAlleles;
        this.genotypes = genotypes;
        this.translation = translation;
    }

    public String getSampleId() {
        return sampleId;
    }

    public void setSampleId(String sampleId) {
        this.sampleId = sampleId;
    }

    public List<StarAlleleResult> getStarAlleles() {
        return starAlleles;
    }

    public void setStarAlleles(List<StarAlleleResult> starAlleles) {
        this.starAlleles = starAlleles;
    }

    public List<Genotype> getGenotypes() {
        return genotypes;
    }

    public void setGenotypes(List<Genotype> genotypes) {
        this.genotypes = genotypes;
    }

    public List<TranslationInfo> getTranslation() {
        return translation;
    }

    public void setTranslation(List<TranslationInfo> translation) {
        this.translation = translation;
    }

    /**
     * Star allele result for a single gene.
     */
    public static class StarAlleleResult {
        @JsonProperty("gene")
        private String gene;

        @JsonProperty("alleles")
        private List<AlleleCall> alleles;

        @JsonProperty("variants")
        private List<String> variants;

        public StarAlleleResult() {
        }

        public StarAlleleResult(String gene, List<AlleleCall> alleles, List<String> variants) {
            this.gene = gene;
            this.alleles = alleles;
            this.variants = variants;
        }

        public String getGene() {
            return gene;
        }

        public void setGene(String gene) {
            this.gene = gene;
        }

        public List<AlleleCall> getAlleles() {
            return alleles;
        }

        public void setAlleles(List<AlleleCall> alleles) {
            this.alleles = alleles;
        }

        public List<String> getVariants() {
            return variants;
        }

        public void setVariants(List<String> variants) {
            this.variants = variants;
        }
    }

    /**
     * Individual allele call.
     */
    public static class AlleleCall {
        @JsonProperty("allele")
        private String allele;

        @JsonProperty("annotation")
        private Object annotation;  // Placeholder for future StarAlleleAnnotation

        public AlleleCall() {
        }

        public AlleleCall(String allele) {
            this.allele = allele;
            this.annotation = null;  // Will be filled by StarAlleleAnnotation
        }

        public AlleleCall(String allele, Object annotation) {
            this.allele = allele;
            this.annotation = annotation;
        }

        public String getAllele() {
            return allele;
        }

        public void setAllele(String allele) {
            this.allele = allele;
        }

        public Object getAnnotation() {
            return annotation;
        }

        public void setAnnotation(Object annotation) {
            this.annotation = annotation;
        }
    }

    /**
     * Genotype information for a single variant.
     */
    public static class Genotype {
        @JsonProperty("variant")
        private String variant;

        @JsonProperty("genotype")
        private String genotype;

        public Genotype() {
        }

        public Genotype(String variant, String genotype) {
            this.variant = variant;
            this.genotype = genotype;
        }

        public String getVariant() {
            return variant;
        }

        public void setVariant(String variant) {
            this.variant = variant;
        }

        public String getGenotype() {
            return genotype;
        }

        public void setGenotype(String genotype) {
            this.genotype = genotype;
        }
    }

    /**
     * Translation information for a single gene.
     */
    public static class TranslationInfo {
        @JsonProperty("gene")
        private String gene;

        @JsonProperty("assays")
        private List<AssayDefinition> assays;

        public TranslationInfo() {
        }

        public TranslationInfo(String gene, List<AssayDefinition> assays) {
            this.gene = gene;
            this.assays = assays;
        }

        public String getGene() {
            return gene;
        }

        public void setGene(String gene) {
            this.gene = gene;
        }

        public List<AssayDefinition> getAssays() {
            return assays;
        }

        public void setAssays(List<AssayDefinition> assays) {
            this.assays = assays;
        }
    }

    /**
     * Assay definition from translation file.
     */
    public static class AssayDefinition {
        @JsonProperty("id")
        private String id;

        @JsonProperty("allele")
        private String allele;

        public AssayDefinition() {
        }

        public AssayDefinition(String id, String allele) {
            this.id = id;
            this.allele = allele;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getAllele() {
            return allele;
        }

        public void setAllele(String allele) {
            this.allele = allele;
        }
    }
}
