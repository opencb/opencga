package org.opencb.opencga.storage.core.utils.iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

abstract class MultiKeyIterator<T> extends CloseableIterator<T> {

    protected final Comparator<T> comparator;
    protected final List<? extends Iterator<T>> iterators;
    protected T next;
    protected T prev;
    protected int count;
    private boolean init = false;

    MultiKeyIterator(Comparator<T> comparator, List<? extends Iterator<T>> iterators) {
        this.comparator = comparator;
        this.iterators = iterators;
        this.iterators.forEach(this::addCloseableOptional);
        next = null;
        prev = null;
        count = 0;
    }

    protected void checkInit() {
        if (!init) {
            init();
            init = true;
        }
    }

    protected abstract void init();

    public abstract void getNext();

    public int getCount() {
        return count;
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
    public T next() {
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
