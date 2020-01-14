package org.opencb.opencga.analysis.variant.genes.knockout.result;

import java.util.LinkedList;
import java.util.List;

public class TranscriptKnockout {

    private String id;
    private String biotype;
    private List<VariantKnockout> variants = new LinkedList<>();

    public TranscriptKnockout() {
    }

    public TranscriptKnockout(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public TranscriptKnockout setId(String id) {
        this.id = id;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public TranscriptKnockout setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public List<VariantKnockout> getVariants() {
        return variants;
    }

    public TranscriptKnockout setVariants(List<VariantKnockout> variants) {
        this.variants = variants;
        return this;
    }

    public TranscriptKnockout addVariant(VariantKnockout variant) {
        this.variants.add(variant);
        return this;
    }
}
