package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.opencb.commons.datastore.core.DataResult;

/**
 * Created on 23/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSearchLoadResult {

    /**
     * Number of processed variants. This number may be larger than the number of loaded variants, in case that some variants are discarded.
     */
    private final long numProcessedVariants;

    /**
     * Number of loaded variants. This number will always be lower equal than {@link #numProcessedVariants}.
     */
    private final long numLoadedVariants;

    /**
     * Number of deleted variants. In case of having deleted variants from the variants backend that may need to be removed
     * from the Search Engine.
     */
    private final long numDeletedVariants;

    public VariantSearchLoadResult(long numProcessedVariants, long numLoadedVariants, long numDeletedVariants) {
        this.numProcessedVariants = numProcessedVariants;
        this.numLoadedVariants = numLoadedVariants;
        this.numDeletedVariants = numDeletedVariants;
    }

    public long getNumProcessedVariants() {
        return numProcessedVariants;
    }

    public long getNumLoadedVariants() {
        return numLoadedVariants;
    }

    public long getNumDeletedVariants() {
        return numDeletedVariants;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("numProcessedVariants", numProcessedVariants)
                .append("numLoadedVariants", numLoadedVariants)
                .append("numDeletedVariants", numDeletedVariants)
                .toString();
    }
}
