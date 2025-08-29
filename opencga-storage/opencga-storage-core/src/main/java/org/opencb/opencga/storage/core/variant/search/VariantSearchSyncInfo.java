package org.opencb.opencga.storage.core.variant.search;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class VariantSearchSyncInfo {
    private final Status status;
    private final Set<Integer> studies;
    private final Map<Integer, Long> statsHash;

    public VariantSearchSyncInfo(Status status) {
        this.status = status;
        this.studies = null;
        this.statsHash = null;
    }

    public VariantSearchSyncInfo(Status status, Set<Integer> studies, Map<Integer, Long> statsHash) {
        this.status = status;
        this.studies = studies;
        this.statsHash = statsHash;
    }

    public Status getStatus() {
        return status;
    }

    public Set<Integer> getStudies() {
        return studies;
    }

    public Map<Integer, Long> getStatsHash() {
        return statsHash;
    }

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
    public enum Status {
        /**
         *
         */
        SYNCHRONIZED("Y"),                           // All elements are synchronized
        NOT_SYNCHRONIZED("N"),                       // Nothing is synchronized
        STATS_NOT_SYNC("S"),                         // VarAnnot sync | Stats not sync | Studies sync
        STATS_UNKNOWN("S?"),                         // VarAnnot sync | Stats unknown  | Studies sync
        STATS_AND_STUDIES_UNKNOWN("??"),             // VarAnnot sync | Stats unknown  | Studies unknown
        STUDIES_UNKNOWN("?");                        // VarAnnot sync | Stats sync     | Studies unknown
        private final String c;

        Status(String c) {
            this.c = c;
        }

        public String key() {
            return c;
        }

        public boolean isUnknown() {
            return this == STUDIES_UNKNOWN || this == STATS_AND_STUDIES_UNKNOWN || this == STATS_UNKNOWN;
        }

        public boolean studiesUnknown() {
            return this == STUDIES_UNKNOWN || this == STATS_AND_STUDIES_UNKNOWN;
        }

        public boolean statsUnknown() {
            return this == STATS_UNKNOWN || this == STATS_AND_STUDIES_UNKNOWN;
        }

        public static Status from(String c) {
            if (c == null || c.isEmpty()) {
                return STUDIES_UNKNOWN;
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
                case "??":
                case "STATS_NOT_SYNCHRONIZED_AND_UNKNOWN":
                    return STATS_AND_STUDIES_UNKNOWN;
                case "S?":
                case "STATS_NOT_SYNC_UNKNOWN":
                    return STATS_UNKNOWN;
                case "?":
                case "UNKNOWN":
                    return STUDIES_UNKNOWN;
                default:
                    throw new IllegalArgumentException("Unknown sync status '" + c + "'. Available values: "
                            + Arrays.toString(Status.values()));
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantSearchSyncInfo{");
        sb.append("status=").append(status);
        sb.append(", studies=").append(studies);
        sb.append(", statsHash=").append(statsHash);
        sb.append('}');
        return sb.toString();
    }
}
