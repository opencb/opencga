package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created on 23/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSearchLoadResult {

    /**
     * Number of processed variants. This number may be lower than the number of loaded variants, in case that some variants are discarded.
     */
    private final long numProcessedVariants;

    /**
     * Number of loaded variants. This number will always be lower equal than {@link #numProcessedVariants}.
     */
    private final long numLoadedVariants;

    VariantSearchLoadResult(long numProcessedVariants, long numLoadedVariants) {
        this.numProcessedVariants = numProcessedVariants;
        this.numLoadedVariants = numLoadedVariants;
    }

    public long getNumProcessedVariants() {
        return numProcessedVariants;
    }

    public long getNumLoadedVariants() {
        return numLoadedVariants;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("numProcessedVariants", numProcessedVariants)
                .append("numLoadedVariants", numLoadedVariants)
                .toString();
    }
}
