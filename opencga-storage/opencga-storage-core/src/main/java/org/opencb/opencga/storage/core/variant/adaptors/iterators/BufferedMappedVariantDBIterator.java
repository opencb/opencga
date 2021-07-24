package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

public class BufferedMappedVariantDBIterator extends DelegatedVariantDBIterator {

    private final UnaryOperator<List<Variant>> map;
    private final int batchSize;
    private final List<Variant> buffer;
    private Iterator<Variant> bufferIterator;
    private int count;

    BufferedMappedVariantDBIterator(VariantDBIterator delegated, UnaryOperator<List<Variant>> map, int batchSize) {
        super(delegated);
        this.map = map;
        this.batchSize = batchSize;
        buffer = new ArrayList<>(batchSize);
        bufferIterator = buffer.iterator();
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public boolean hasNext() {
        if (bufferIterator.hasNext()) {
            return true;
        } else {
            // Read new buffer
            buffer.clear();
            for (int i = 0; i < batchSize && getDelegated().hasNext(); i++) {
                buffer.add(getDelegated().next());
            }
            bufferIterator = map.apply(buffer).iterator();
            return bufferIterator.hasNext();
        }
    }

    @Override
    public Variant next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        count++;
        return bufferIterator.next();
    }
}
