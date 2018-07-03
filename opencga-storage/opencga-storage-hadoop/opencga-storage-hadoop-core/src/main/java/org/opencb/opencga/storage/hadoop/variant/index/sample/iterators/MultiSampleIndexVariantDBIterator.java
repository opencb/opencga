package org.opencb.opencga.storage.hadoop.variant.index.sample.iterators;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class MultiSampleIndexVariantDBIterator extends SampleIndexVariantDBIterator {

    protected final List<VariantDBIterator> iterators;
    protected Variant next;
    protected Variant prev;

    public MultiSampleIndexVariantDBIterator(List<VariantDBIterator> iterators) {
        this.iterators = iterators;
        next = null;
        prev = null;
        count = 0;
    }

    @Override
    public long getTimeFetching() {
        return iterators.stream().mapToLong(VariantDBIterator::getTimeFetching).sum();
    }

    @Override
    public long getTimeConverting() {
        return iterators.stream().mapToLong(VariantDBIterator::getTimeConverting).sum();
    }

    public abstract void getNext();

    @Override
    public boolean hasNext() {
        if (next == null) {
            if (prev == null) {
                return false;
            }
            getNext();
        }
        return next != null;
    }

    @Override
    public Variant next() {
        if (next == null) {
            getNext();
        }
        if (next == null) {
            throw new NoSuchElementException("No more elements found");
        }
        count++;
        prev = this.next;
        this.next = null;
        return prev;
    }

}
