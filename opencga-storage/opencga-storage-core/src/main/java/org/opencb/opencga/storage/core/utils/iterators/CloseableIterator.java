package org.opencb.opencga.storage.core.utils.iterators;

import com.google.common.collect.Iterators;
import org.opencb.commons.datastore.core.QueryOptions;

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

    public CloseableIterator<T> localLimitSkip(QueryOptions options) {
        return localLimitSkip(options.getInt(QueryOptions.LIMIT, -1), options.getInt(QueryOptions.SKIP, -1));
    }

    public CloseableIterator<T> localLimitSkip(int limit, int skip) {
        // Client site limit-skip
        if (skip > 0) {
            Iterators.advance(this, skip);
        }
        if (limit >= 0) {
            Iterator<T> it = Iterators.limit(this, limit);
            return wrap(it).addCloseable(this);
        } else {
            return this;
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

    public static <T> CloseableIterator<T> wrap(Iterator<T> iterator, AutoCloseable... closeables) {
        return new CloseableIterator<T>() {
            {
                for (AutoCloseable closeable : closeables) {
                    addCloseable(closeable);
                }
            }
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }
        };
    }
}
