package org.opencb.opencga.storage.core.alignment.iterators;

import java.util.Iterator;

/**
 * Created by pfurio on 26/10/16.
 */
public abstract class AlignmentIterator<T> implements Iterator<T>, AutoCloseable {

    public AlignmentIterator() {
    }

}
