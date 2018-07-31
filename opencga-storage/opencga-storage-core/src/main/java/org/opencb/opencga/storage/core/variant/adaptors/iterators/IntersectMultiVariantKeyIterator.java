package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IntersectMultiVariantKeyIterator extends MultiVariantKeyIterator {

    private final List<VariantDBIterator> negatedIterators;
    private final List<Variant> negatedVariants;
    protected boolean firstVariant = true;

    public IntersectMultiVariantKeyIterator(List<VariantDBIterator> iterators) {
        this(iterators, Collections.emptyList());
    }

    public IntersectMultiVariantKeyIterator(List<VariantDBIterator> iterators, List<VariantDBIterator> negatedIterators) {
        super(iterators);
        this.negatedIterators = negatedIterators;
        negatedIterators.forEach(this::addCloseable);
        negatedVariants = new ArrayList<>(negatedIterators.size());
    }

    @Override
    protected void init() {
        for (VariantDBIterator negatedIterator : negatedIterators) {
            if (negatedIterator.hasNext()) {
                negatedVariants.add(negatedIterator.next());
            } else {
                negatedVariants.add(null);
            }
        }

        // Get first variant of the first iterator to initialize the loop
        VariantDBIterator iterator = this.iterators.get(0);
        if (iterator.hasNext()) {
            prev = iterator.next();
        }
    }

    @Override
    public void getNext() {

        boolean existsInNegatedIterators;
        Variant target;
        do {
            target = nextMatch();
            existsInNegatedIterators = existsInNegatedIterators(target);
        } while (target != null && existsInNegatedIterators);

        prev = null;
        next = target;
    }

    /**
     * Finds next variant that is present in all the iterators.
     *
     * @return Variant in all iterators, or null
     */
    protected Variant nextMatch() {

        // Find target variant
        Variant target;
        if (firstVariant) {
            target = prev;
            firstVariant = false;
        } else {
            if (iterators.get(0).hasNext()) {
                target = iterators.get(0).next();
            } else {
                return null;
            }
        }

        // Selected target variant is from the first iterator.
        // Number of iterators with the target variant. Start with one match.
        int numMatches = 1;
        int i = 0; // Incrementing this index at the beginning of the loop. Skip first iterator

        // Iterate while the number of matches is the same as the number of iterators, so all the
        loop:
        while (numMatches != iterators.size()) {
            i++;
            i %= iterators.size();
            VariantDBIterator iterator = iterators.get(i);
            Variant variant;

            // Iterate until find a variant equals or above to the target variant
            do {
                if (iterator.hasNext()) {
                    variant = iterator.next();
                } else {
                    // End of the loop. Finish after first empty iterator
                    target = null;
                    break loop;
                }
            } while (VARIANT_COMPARATOR.compare(variant, target) < 0);
            // If same variant, we have another match!
            if (target.sameGenomicVariant(variant)) {
                numMatches++;
//                System.out.println("Iterator " + i + " matches with target '" + target + "'! Num matches : " + numMatches);
            } else {
                // If is not the target variant, change the target, and reset the number of matches
                target = variant;
                numMatches = 1;
//                System.out.println("Iterator " + i + " does not match with target. New target : " + target);
            }
        }
        return target;
    }

    /**
     * Check against all negatedIterators if any of them contains the target variant.
     * If so, this variant should be discarded
     *
     * @param target Target variant to check
     * @return  If the variant is valid
     */
    protected boolean existsInNegatedIterators(Variant target) {
        boolean exists = false;
        if (target != null) {
            for (int i = 0; i < negatedIterators.size(); i++) {
                VariantDBIterator negatedIterator = negatedIterators.get(i);
                Variant variant = negatedVariants.get(i);
                while (variant != null && VARIANT_COMPARATOR.compare(variant, target) < 0) {
                    if (negatedIterator.hasNext()) {
                        variant = negatedIterator.next();
                    } else {
                        variant = null;
                    }
                    negatedVariants.set(i, variant);
                }
                if (variant != null && variant.sameGenomicVariant(target)) {
                    exists = true;
                    break;
                }
            }
        }
        return exists;
    }


}
