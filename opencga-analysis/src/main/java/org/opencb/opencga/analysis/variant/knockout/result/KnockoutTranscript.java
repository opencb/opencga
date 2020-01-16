package org.opencb.opencga.analysis.variant.knockout.result;

import java.util.LinkedList;
import java.util.List;

public class KnockoutTranscript {

    private String id;
    private String biotype;
    private List<KnockoutVariant> variants = new LinkedList<>();

    public KnockoutTranscript() {
    }

    public KnockoutTranscript(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public KnockoutTranscript setId(String id) {
        this.id = id;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public KnockoutTranscript setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public List<KnockoutVariant> getVariants() {
        return variants;
    }

    public KnockoutTranscript setVariants(List<KnockoutVariant> variants) {
        this.variants = variants;
        return this;
    }

    public KnockoutTranscript addVariant(KnockoutVariant variant) {
        this.variants.add(variant);
        return this;
    }
}
