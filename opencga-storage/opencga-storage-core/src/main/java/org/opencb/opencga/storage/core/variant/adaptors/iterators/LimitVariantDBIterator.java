package org.opencb.opencga.storage.core.variant.adaptors.iterators;

public class LimitVariantDBIterator extends DelegatedVariantDBIterator {

    private final int limit;

    LimitVariantDBIterator(VariantDBIterator delegated, int limit) {
        super(delegated);
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        return getCount() < limit && super.hasNext();
    }
}
