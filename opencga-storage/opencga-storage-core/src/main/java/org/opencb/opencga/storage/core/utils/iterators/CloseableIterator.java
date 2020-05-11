package org.opencb.opencga.storage.core.utils.iterators;

import java.io.IOException;
import java.util.*;

public abstract class CloseableIterator<T> implements Iterator<T>, AutoCloseable {

    public static final EmptyCloseableIterator EMPTY_CLOSEABLE_ITERATOR = new EmptyCloseableIterator();
    private List<AutoCloseable> closeables = new ArrayList<>();

    public static <T> CloseableIterator<T> emptyIterator() {
        return EMPTY_CLOSEABLE_ITERATOR;
    }

    public CloseableIterator<T> addCloseable(AutoCloseable closeable) {
        this.closeables.add(closeable);
        return this;
    }

    public CloseableIterator<T> addCloseableOptional(Object object) {
        if (object instanceof AutoCloseable) {
            this.closeables.add(((AutoCloseable) object));
        }
        return this;
    }

    @Override
    public void close() throws Exception {
        List<Exception> exceptions = new LinkedList<>();
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        if (exceptions.size() == 1) {
            throw exceptions.get(0);
        } else if (exceptions.size() > 1) {
            IOException e = new IOException("Errors found closing iterator");
            exceptions.forEach(e::addSuppressed);
            throw e;
        }
    }

    private static class EmptyCloseableIterator<T> extends CloseableIterator<T> {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException("empty iterator");
        }
    }
}
