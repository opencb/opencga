package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.*;

/**
 * Created by jacobo on 28/03/19.
 */
public class VariantDBIteratorWithCounts extends DelegatedVariantDBIterator {

    private final LinkedHashMap<String, Integer> chrCount;
    private String chromosome;

    public VariantDBIteratorWithCounts(VariantDBIterator iterator) {
        super(iterator);
        chrCount = new LinkedHashMap<>();
    }

    @Override
    public Variant next() {
        Variant next = super.next();
        chromosome = next.getChromosome();
        chrCount.merge(chromosome, 1, Integer::sum);
        return next;
    }

    public Map<String, Integer> getChromosomeCount() {
        return chrCount;
    }

    public Integer getChromosomeCount(String chromosome) {
        return chrCount.get(chromosome);
    }

    public Integer getNumChromosomes() {
        return chrCount.size();
    }

    public List<String> getChromosomes() {
        return new ArrayList<>(chrCount.keySet());
    }

    public String getCurrentChromosome() {
        return chromosome;
    }
}
