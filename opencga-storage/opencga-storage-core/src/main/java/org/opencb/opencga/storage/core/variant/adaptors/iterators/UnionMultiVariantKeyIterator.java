package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UnionMultiVariantKeyIterator extends MultiVariantKeyIterator {

    protected List<Variant> variants;

    public UnionMultiVariantKeyIterator(List<VariantDBIterator> iterators) {
        super(iterators);
        variants = new ArrayList<>(iterators.size());
    }

    @Override
    protected void init() {
        for (VariantDBIterator iterator : iterators) {
            if (iterator.hasNext()) {
                variants.add(iterator.next());
            } else {
                // Mark as this iterator is over
                variants.add(null);
            }
        }
        next = variants.stream().filter(Objects::nonNull).min(VARIANT_COMPARATOR).orElse(null);
    }

    @Override
    public void getNext() {

        Variant nextMin = null;
        //System.out.println("variants = " + variants);
        //System.out.println("prev = " + prev);

        // Find the smallest variant
        for (int i = 0; i < iterators.size(); i++) {
            VariantDBIterator iterator = iterators.get(i);
            Variant variant = variants.get(i);
            // Increment iterator if it was pointing to the previous variant
            if (prev.sameGenomicVariant(variant)) {
                if (iterator.hasNext()) {
                    variant = iterator.next();
                    //System.out.println("variants[" + i + "] = " + variant);
                    variants.set(i, variant);
                } else {
                    //System.out.println("variants[" + i + "] = " + null);
                    variant = null;
                    variants.set(i, null);
                }
            }
            if (variant != null) {
                if (nextMin == null) {
                    nextMin = variant;
                } else {
                    if (/*currentChromosome.equals(variant.getChromosome()) && */
                            VARIANT_COMPARATOR.compare(variant, nextMin) < 0) {
                        nextMin = variant;
                    }
                }
            }
        }


        prev = null;
        next = nextMin;

        //System.out.println("next = " + next);
    }

}
