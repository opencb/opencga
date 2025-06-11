package org.opencb.opencga.storage.core.variant.search;

import java.util.Arrays;

/**
 * Variant secondary annotation index synchronization status.
 * <p>
 * There are 3 elements to synchronize within a variant.
 * - VariantAnnotation
 * - VariantStats
 * - studies
 * <p>
 * The status of the synchronization is stored in the VariantAnnotation.additionalAttributes
 */
public enum VariantSearchSyncStatus {
    /**
     *
     */
    SYNCHRONIZED("Y"),                           // All elements are synchronized
    NOT_SYNCHRONIZED("N"),                       // Nothing is synchronized
    STATS_NOT_SYNC("S"),                         // VarAnnot sync | Stats not sync | Studies sync
    STATS_NOT_SYNC_AND_STUDIES_UNKNOWN("S?"),    // VarAnnot sync | Stats not sync | Studies unknown
    STUDIES_UNKNOWN_SYNC("?");                   // VarAnnot sync | Stats sync     | Studies unknown
    private final String c;

    VariantSearchSyncStatus(String c) {
        this.c = c;
    }

    public String key() {
        return c;
    }

    public static VariantSearchSyncStatus from(String c) {
        if (c == null || c.isEmpty()) {
            return STUDIES_UNKNOWN_SYNC;
        }
        switch (c) {
            case "Y":
            case "SYNCHRONIZED":
                return SYNCHRONIZED;
            case "N":
            case "NOT_SYNCHRONIZED":
                return NOT_SYNCHRONIZED;
            case "S":
            case "STATS_NOT_SYNCHRONIZED":
                return STATS_NOT_SYNC;
            case "S?":
            case "STATS_NOT_SYNCHRONIZED_AND_UNKNOWN":
                return STATS_NOT_SYNC_AND_STUDIES_UNKNOWN;
            case "?":
            case "UNKNOWN":
                return STUDIES_UNKNOWN_SYNC;
            default:
                throw new IllegalArgumentException("Unknown sync status '" + c + "'. Available values: "
                        + Arrays.toString(VariantSearchSyncStatus.values()));
        }
    }
}
