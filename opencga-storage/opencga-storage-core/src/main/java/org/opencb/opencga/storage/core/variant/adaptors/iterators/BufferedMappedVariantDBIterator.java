package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.*;
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
        bufferIterator = Collections.emptyIterator();
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public boolean hasNext() {
        while (!bufferIterator.hasNext()) {
            if (!getDelegated().hasNext()) {
                return false;
            }
            // Read new buffer
            buffer.clear();
            for (int i = 0; i < batchSize && getDelegated().hasNext(); i++) {
                buffer.add(getDelegated().next());
            }
            // Mapper might clear up the buffer. In that case, need to fetch more items
            bufferIterator = map.apply(buffer).iterator();
        }
        return true;
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
