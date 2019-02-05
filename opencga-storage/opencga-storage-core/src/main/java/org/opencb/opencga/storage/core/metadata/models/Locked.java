package org.opencb.opencga.storage.core.metadata.models;

import java.io.Closeable;

/**
 * Reference to a locked element.
 *
 * Created on 05/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface Locked extends Closeable {

    /**
     * Unlock the locked element.
     */
    void unlock();

    @Override
    default void close() {
        unlock();
    }
}
