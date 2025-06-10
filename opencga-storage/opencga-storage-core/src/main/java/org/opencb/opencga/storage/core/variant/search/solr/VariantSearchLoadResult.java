package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

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
     * Number of loaded variants. Includes inserted and partially updated variants.
     *
     * This number will always be lower equal than {@link #numProcessedVariants}.
     */
    private final long numLoadedVariants;

    /**
     * Number of deleted variants. In case of having deleted variants from the variants backend that may need to be removed
     * from the Search Engine.
     */
    private final long numDeletedVariants;

    /**
     * Number of inserted variants. This number will always be lower equal than {@link #numLoadedVariants}.
     *
     * This number does not include variants that were partially updated.
     */
    private final long numInsertedVariants;

    /**
     * Number of variants that were partially updated. This number will always be lower equal than {@link #numLoadedVariants}.
     *
     * This number does not include variants that were inserted.
     */
    private final long numLoadedVariantsPartialStatsUpdate;

    public VariantSearchLoadResult(long numProcessedVariants, long numLoadedVariants, long numDeletedVariants,
                                   long numInsertedVariants, long numLoadedVariantsPartialStatsUpdate) {
        this.numProcessedVariants = numProcessedVariants;
        this.numLoadedVariants = numLoadedVariants;
        this.numDeletedVariants = numDeletedVariants;
        this.numInsertedVariants = numInsertedVariants;
        this.numLoadedVariantsPartialStatsUpdate = numLoadedVariantsPartialStatsUpdate;
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

    public long getNumInsertedVariants() {
        return numInsertedVariants;
    }

    public long getNumLoadedVariantsPartialStatsUpdate() {
        return numLoadedVariantsPartialStatsUpdate;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("numProcessedVariants", numProcessedVariants)
                .append("numLoadedVariants", numLoadedVariants)
                .append("numDeletedVariants", numDeletedVariants)
                .append("numLoadedVariantsMain", numInsertedVariants)
                .append("numLoadedVariantsStats", numLoadedVariantsPartialStatsUpdate)
                .toString();
    }
}
