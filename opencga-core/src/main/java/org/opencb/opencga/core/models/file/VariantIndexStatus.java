package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.InternalStatus;

import java.util.Arrays;
import java.util.List;

public class VariantIndexStatus extends InternalStatus {

    /*
     * States
     *
     * NONE --> TRANSFORMING --> TRANSFORMED --> LOADING --> READY
     *      \                                              /
     *       ------------------> INDEXING ----------------/
     *
     */
    public static final String NONE = "NONE";
    public static final String TRANSFORMING = "TRANSFORMING";
    public static final String TRANSFORMED = "TRANSFORMED";
    public static final String LOADING = "LOADING";
    public static final String INDEXING = "INDEXING";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, NONE, TRANSFORMED, TRANSFORMING, LOADING, INDEXING);

    public VariantIndexStatus(String status, String message) {
        if (isValid(status)) {
            init(status, status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public VariantIndexStatus(String status) {
        this(status, "");
    }

    public VariantIndexStatus() {
        this(NONE, "");
    }

    @Override
    public String getId() {
        return super.getId();
    }

    @Override
    public String getName() {
        return super.getName();
    }

    public static VariantIndexStatus init() {
        return new VariantIndexStatus();
    }

    public static boolean isValid(String status) {
        if (InternalStatus.isValid(status)) {
            return true;
        }
        if (status != null && (status.equals(NONE) || status.equals(TRANSFORMING) || status.equals(TRANSFORMED)
                || status.equals(LOADING) || status.equals(INDEXING))) {
            return true;
        }
        return false;
    }
}
