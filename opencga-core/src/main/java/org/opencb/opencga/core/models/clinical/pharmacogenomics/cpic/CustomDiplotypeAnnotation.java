package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

/**
 * Custom diplotype annotation from an external file mapping allele pairs to clinical function.
 */
public class CustomDiplotypeAnnotation {

    private String gene;
    private String allele1;
    private String allele2;
    private String function;
    private String description;
    private String type;

    public CustomDiplotypeAnnotation() {
    }

    public CustomDiplotypeAnnotation(String gene, String allele1, String allele2,
                                     String function, String description, String type) {
        this.gene = gene;
        this.allele1 = allele1;
        this.allele2 = allele2;
        this.function = function;
        this.description = description;
        this.type = type;
    }

    public String getGene() { return gene; }
    public CustomDiplotypeAnnotation setGene(String gene) { this.gene = gene; return this; }

    public String getAllele1() { return allele1; }
    public CustomDiplotypeAnnotation setAllele1(String allele1) { this.allele1 = allele1; return this; }

    public String getAllele2() { return allele2; }
    public CustomDiplotypeAnnotation setAllele2(String allele2) { this.allele2 = allele2; return this; }

    public String getFunction() { return function; }
    public CustomDiplotypeAnnotation setFunction(String function) { this.function = function; return this; }

    public String getDescription() { return description; }
    public CustomDiplotypeAnnotation setDescription(String description) { this.description = description; return this; }

    public String getType() { return type; }
    public CustomDiplotypeAnnotation setType(String type) { this.type = type; return this; }
}
