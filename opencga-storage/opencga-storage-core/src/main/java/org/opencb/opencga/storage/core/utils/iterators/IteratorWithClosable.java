package org.opencb.opencga.storage.core.utils.iterators;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IteratorWithClosable<T> implements Iterator<T> {
    private final Iterator<T> iterator;
    private final Closeable closable;

    public IteratorWithClosable(Iterator<T> iterator, Closeable closable) {
        this.iterator = iterator;
        this.closable = closable;
    }

    @Override
    public boolean hasNext() {
        if (!iterator.hasNext()) {
            try {
                closable.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return false;
        } else {
            return true;
        }
    }

    @Override
    public T next() {
        return iterator.next();
    }
}
