package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.biodata.models.variant.Variant;

import java.util.Iterator;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class VariantDBIterator implements Iterator<Variant> {
    protected long timeFetching = 0;
    protected long timeConverting = 0;

    public long getTimeConverting() {
        return timeConverting;
    }

    public void setTimeFetching(long timeFetching) {
        this.timeFetching = timeFetching;
    }

    public long getTimeFetching() {
        return timeFetching;
    }

    public void setTimeConverting(long timeConverting) {
        this.timeConverting = timeConverting;
    }
}
