package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.Iterator;

class VariantDBIteratorWrapper extends VariantDBIterator {
    protected final Iterator<Variant> iterator;
    protected int count = 0;

    VariantDBIteratorWrapper(Iterator<Variant> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return fetch(iterator::hasNext);
    }

    @Override
    public Variant next() {
        count++;
        return fetch(iterator::next);
    }

    @Override
    public int getCount() {
        return count;
    }
}
