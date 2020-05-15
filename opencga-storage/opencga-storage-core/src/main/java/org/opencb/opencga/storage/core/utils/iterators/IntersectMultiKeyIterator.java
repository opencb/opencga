package org.opencb.opencga.storage.core.utils.iterators;

import java.util.*;

public class IntersectMultiKeyIterator<T> extends MultiKeyIterator<T> {

    private final List<? extends Iterator<T>> negatedIterators;
    private final List<T> negatedTs;
    protected boolean firstElement = true;

    public IntersectMultiKeyIterator(Comparator<T> comparator, List<? extends Iterator<T>> iterators) {
        this(comparator, iterators, Collections.emptyList());
    }

    public IntersectMultiKeyIterator(Comparator<T> comparator,
                                     List<? extends Iterator<T>> iterators,
                                     List<? extends Iterator<T>> negatedIterators) {
        super(comparator, iterators);
        this.negatedIterators = negatedIterators;
        negatedIterators.forEach(this::addCloseableOptional);
        negatedTs = new ArrayList<>(negatedIterators.size());
    }

    @Override
    protected void init() {
        for (Iterator<T> negatedIterator : negatedIterators) {
            if (negatedIterator.hasNext()) {
                negatedTs.add(negatedIterator.next());
            } else {
                negatedTs.add(null);
            }
        }

        // Get first element of the first iterator to initialize the loop
        Iterator<T> iterator = this.iterators.get(0);
        if (iterator.hasNext()) {
            prev = iterator.next();
        }
    }

    @Override
    public void getNext() {

        boolean existsInNegatedIterators;
        T target;
        do {
            target = nextMatch();
            existsInNegatedIterators = existsInNegatedIterators(target);
        } while (target != null && existsInNegatedIterators);

        prev = null;
        next = target;
    }

    /**
     * Finds next element that is present in all the iterators.
     *
     * @return element in all iterators, or null
     */
    protected T nextMatch() {

        // Find target element
        T target;
        if (firstElement) {
            target = prev;
            firstElement = false;
        } else {
            if (iterators.get(0).hasNext()) {
                target = iterators.get(0).next();
            } else {
                return null;
            }
        }

        // Selected target element is from the first iterator.
        // Number of iterators with the target element. Start with one match.
        int numMatches = 1;
        int i = 0; // Incrementing this index at the beginning of the loop. Skip first iterator

        // Iterate while the number of matches is the same as the number of iterators, so all the
        loop:
        while (numMatches != iterators.size()) {
            i++;
            i %= iterators.size();
            Iterator<T> iterator = iterators.get(i);
            T t;

            // Iterate until find a t equals or above to the target t
            do {
                if (iterator.hasNext()) {
                    t = iterator.next();
                } else {
                    // End of the loop. Finish after first empty iterator
                    target = null;
                    break loop;
                }
            } while (comparator.compare(t, target) < 0);
            // If same t, we have another match!
            if (comparator.compare(t, target) == 0) {
                numMatches++;
//                System.out.println("Iterator " + i + " matches with target '" + target + "'! Num matches : " + numMatches);
            } else {
                // If is not the target t, change the target, and reset the number of matches
                target = t;
                numMatches = 1;
//                System.out.println("Iterator " + i + " does not match with target. New target : " + target);
            }
        }
        return target;
    }

    /**
     * Check against all negatedIterators if any of them contains the target element.
     * If so, this element should be discarded
     *
     * @param target Target element to check
     * @return  If the element is valid
     */
    protected boolean existsInNegatedIterators(T target) {
        boolean exists = false;
        if (target != null) {
            for (int i = 0; i < negatedIterators.size(); i++) {
                Iterator<T> negatedIterator = negatedIterators.get(i);
                T t = negatedTs.get(i);
                while (t != null && comparator.compare(t, target) < 0) {
                    if (negatedIterator.hasNext()) {
                        t = negatedIterator.next();
                    } else {
                        t = null;
                    }
                    negatedTs.set(i, t);
                }
                if (t != null && comparator.compare(t, target) == 0) {
                    exists = true;
                    break;
                }
            }
        }
        return exists;
    }

}
