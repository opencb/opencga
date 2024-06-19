package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.NoSuchElementException;

public class LimitVariantDBIterator extends DelegatedVariantDBIterator {

    private final int limit;
    private int count;

    LimitVariantDBIterator(VariantDBIterator delegated, int limit) {
        super(delegated);
        this.limit = limit;
        this.count = 0;
    }

    @Override
    public boolean hasNext() {
        return count < limit && super.hasNext();
    }

    @Override
    public Variant next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        } else {
            ++this.count;
            return super.next();
        }
    }
}
