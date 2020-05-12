package org.opencb.opencga.storage.core.utils.iterators;

import java.util.*;

public class UnionMultiKeyIterator<T> extends MultiKeyIterator<T> {

    protected List<T> elements;

    public UnionMultiKeyIterator(Comparator<T> comparator, List<? extends Iterator<T>> iterators) {
        super(comparator, iterators);
        elements = new ArrayList<>(iterators.size());
    }

    @Override
    protected void init() {
        for (Iterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                elements.add(iterator.next());
            } else {
                // Mark as this iterator is over
                elements.add(null);
            }
        }
        next = elements.stream().filter(Objects::nonNull).min(comparator).orElse(null);
    }

    @Override
    public void getNext() {
        T nextMin = null;

        // Find the smallest element
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<T> iterator = iterators.get(i);
            T element = elements.get(i);
            // Increment iterator if it was pointing to the previous element
            if (element != null && comparator.compare(prev, element) == 0) {
                if (iterator.hasNext()) {
                    element = iterator.next();
                    elements.set(i, element);
                } else {
                    element = null;
                    elements.set(i, null);
                }
            }
            if (element != null) {
                if (nextMin == null) {
                    nextMin = element;
                } else {
                    if (comparator.compare(element, nextMin) < 0) {
                        nextMin = element;
                    }
                }
            }
        }

        prev = null;
        next = nextMin;
    }

}
