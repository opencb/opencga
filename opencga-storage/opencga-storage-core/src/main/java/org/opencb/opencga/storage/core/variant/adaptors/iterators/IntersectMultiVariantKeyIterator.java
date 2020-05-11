package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.opencga.storage.core.utils.iterators.IntersectMultiKeyIterator;

import java.util.List;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IntersectMultiVariantKeyIterator extends VariantDBIteratorWrapper {

    private final List<VariantDBIterator> iterators;
    private final List<VariantDBIterator> negatedIterators;

    public IntersectMultiVariantKeyIterator(List<VariantDBIterator> iterators, List<VariantDBIterator> negatedIterators) {
        super(new IntersectMultiKeyIterator<>(VariantDBIterator.VARIANT_COMPARATOR, iterators, negatedIterators));
        this.iterators = iterators;
        this.negatedIterators = negatedIterators;
        this.iterators.forEach(this::addCloseable);
        this.negatedIterators.forEach(this::addCloseable);
    }

    @Override
    public long getTimeFetching() {
        return iterators.stream().mapToLong(VariantDBIterator::getTimeFetching).sum();
    }

    @Override
    public long getTimeConverting() {
        return iterators.stream().mapToLong(VariantDBIterator::getTimeConverting).sum();
    }

}
