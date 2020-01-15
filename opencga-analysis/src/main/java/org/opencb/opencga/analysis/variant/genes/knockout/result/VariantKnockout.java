package org.opencb.opencga.analysis.variant.genes.knockout.result;

import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;

import java.util.List;

public class VariantKnockout {

    private String variant;
    private String genotype;
    private String filter;
    private String qual;
    private KnockoutType knockoutType;
    private List<SequenceOntologyTerm> sequenceOntologyTerms;

    public enum KnockoutType {
        HOM_ALT,
        COMP_HET,
        MULTI_ALLELIC,
        DELETION_OVERLAP
    }

    public VariantKnockout() {
    }

    public VariantKnockout(String variant, String genotype, String filter, String qual, KnockoutType knockoutType,
                           List<SequenceOntologyTerm> sequenceOntologyTerms) {
        this.variant = variant;
        this.genotype = genotype;
        this.filter = filter;
        this.qual = qual;
        this.knockoutType = knockoutType;
        this.sequenceOntologyTerms = sequenceOntologyTerms;
    }

    public String getVariant() {
        return variant;
    }

    public VariantKnockout setVariant(String variant) {
        this.variant = variant;
        return this;
    }

    public String getGenotype() {
        return genotype;
    }

    public VariantKnockout setGenotype(String genotype) {
        this.genotype = genotype;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public VariantKnockout setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public VariantKnockout setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public KnockoutType getKnockoutType() {
        return knockoutType;
    }

    public VariantKnockout setKnockoutType(KnockoutType knockoutType) {
        this.knockoutType = knockoutType;
        return this;
    }

    public List<SequenceOntologyTerm> getSequenceOntologyTerms() {
        return sequenceOntologyTerms;
    }

    public VariantKnockout setSequenceOntologyTerms(List<SequenceOntologyTerm> sequenceOntologyTerms) {
        this.sequenceOntologyTerms = sequenceOntologyTerms;
        return this;
    }
}
