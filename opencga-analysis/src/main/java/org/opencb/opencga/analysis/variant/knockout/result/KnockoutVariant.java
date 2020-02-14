package org.opencb.opencga.analysis.variant.knockout.result;

import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;

import java.util.List;

public class KnockoutVariant {

    private String id;
    private String genotype;
    private String filter;
    private String qual;
    private KnockoutType knockoutType;
    private List<SequenceOntologyTerm> sequenceOntologyTerms;

    public enum KnockoutType {
        HOM_ALT,
        COMP_HET,
        HET_ALT,
        DELETION_OVERLAP
    }

    public KnockoutVariant() {
    }

    public KnockoutVariant(String id, String genotype, String filter, String qual, KnockoutType knockoutType,
                           List<SequenceOntologyTerm> sequenceOntologyTerms) {
        this.id = id;
        this.genotype = genotype;
        this.filter = filter;
        this.qual = qual;
        this.knockoutType = knockoutType;
        this.sequenceOntologyTerms = sequenceOntologyTerms;
    }

    public String getId() {
        return id;
    }

    public KnockoutVariant setId(String id) {
        this.id = id;
        return this;
    }

    public String getGenotype() {
        return genotype;
    }

    public KnockoutVariant setGenotype(String genotype) {
        this.genotype = genotype;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public KnockoutVariant setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public KnockoutVariant setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public KnockoutType getKnockoutType() {
        return knockoutType;
    }

    public KnockoutVariant setKnockoutType(KnockoutType knockoutType) {
        this.knockoutType = knockoutType;
        return this;
    }

    public List<SequenceOntologyTerm> getSequenceOntologyTerms() {
        return sequenceOntologyTerms;
    }

    public KnockoutVariant setSequenceOntologyTerms(List<SequenceOntologyTerm> sequenceOntologyTerms) {
        this.sequenceOntologyTerms = sequenceOntologyTerms;
        return this;
    }
}
