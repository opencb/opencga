package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.opencga.storage.core.utils.iterators.UnionMultiKeyIterator;

import java.util.List;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class UnionMultiVariantKeyIterator extends VariantDBIteratorWrapper {

    private final List<VariantDBIterator> iterators;

    public UnionMultiVariantKeyIterator(List<VariantDBIterator> iterators) {
        super(new UnionMultiKeyIterator<>(VariantDBIterator.VARIANT_COMPARATOR, iterators));
        this.iterators = iterators;
        this.iterators.forEach(this::addCloseable);
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
