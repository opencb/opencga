package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
abstract class MultiVariantKeyIterator extends VariantDBIterator {

    protected static final Comparator<Variant> VARIANT_COMPARATOR = Comparator.comparing(Variant::getChromosome)
            .thenComparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(Variant::toString);

    protected final List<VariantDBIterator> iterators;
    protected Variant next;
    protected Variant prev;
    protected int count;
    private boolean init = false;

    MultiVariantKeyIterator(List<VariantDBIterator> iterators) {
        this.iterators = iterators;
        next = null;
        prev = null;
        count = 0;
        iterators.forEach(this::addCloseable);
    }

    protected void checkInit() {
        if (!init) {
            init();
            init = true;
        }
    }

    protected abstract void init();

    public abstract void getNext();

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public long getTimeFetching() {
        return iterators.stream().mapToLong(VariantDBIterator::getTimeFetching).sum();
    }

    @Override
    public long getTimeConverting() {
        return iterators.stream().mapToLong(VariantDBIterator::getTimeConverting).sum();
    }

    @Override
    public boolean hasNext() {
        checkInit();
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
        checkInit();
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
