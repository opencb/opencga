package org.opencb.opencga.storage.hadoop.variant.index.sample.iterators;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class SampleIndexVariantDBIterator extends VariantDBIterator {

    protected int count = 0;

    /**
     * @return Number of returned variants
     */
    public int getCount() {
        return count;
    }
}
