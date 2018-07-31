package org.opencb.opencga.storage.hadoop.variant.gaps;

/**
 * Created on 02/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public enum VariantOverlappingStatus {
    /**
     * A variant from this file is overlapping with this variant.
     */
    VARIANT("V"),
    /**
     * A reference block from this file is overlapping with this variant.
     */
    REFERENCE("R"),
    /**
     * There was a gap in the original file.
     */
    GAP("G"),
    /**
     *
     */
    MULTI("M"),
    /**
     * This variant is present on this file.
     */
    NONE("N");

    private final String v;

    VariantOverlappingStatus(String v) {
        this.v = v;
    }

    public String toString() {
        return v;
    }
}
