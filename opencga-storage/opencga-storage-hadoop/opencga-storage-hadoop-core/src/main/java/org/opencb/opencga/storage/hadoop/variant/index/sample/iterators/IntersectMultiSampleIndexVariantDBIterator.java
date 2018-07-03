package org.opencb.opencga.storage.hadoop.variant.index.sample.iterators;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConverter.VARIANT_COMPARATOR;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IntersectMultiSampleIndexVariantDBIterator extends MultiSampleIndexVariantDBIterator {

    protected boolean firstVariant = true;

    public IntersectMultiSampleIndexVariantDBIterator(List<VariantDBIterator> iterators) {
        super(iterators);
        init();
    }

    protected void init() {
        // Get first variant of the first iterator to initialize the loop
        VariantDBIterator iterator = iterators.get(0);
        if (iterator.hasNext()) {
            prev = iterator.next();
        }
    }

    @Override
    public void getNext() {

        // Find target variant
        Variant target;
        if (firstVariant) {
            target = prev;
            firstVariant = false;
        } else {
            if (iterators.get(0).hasNext()) {
                target = iterators.get(0).next();
            } else {
                prev = null;
                next = null;
                return;
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

        prev = null;
        next = target;
    }


}
