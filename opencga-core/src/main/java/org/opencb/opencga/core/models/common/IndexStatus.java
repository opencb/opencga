package org.opencb.opencga.core.models.common;

import java.util.Arrays;
import java.util.List;

public class IndexStatus extends InternalStatus {

    /*
     * States
     *
     * NONE --> INDEXING --> READY
     *
     */
    public static final String NONE = "NONE";
    public static final String INDEXING = "INDEXING";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, NONE, INDEXING);

    public IndexStatus(String status, String message) {
        if (isValid(status)) {
            init(status, status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public IndexStatus(String status) {
        this(status, "");
    }

    public IndexStatus() {
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

    public static IndexStatus init() {
        return new IndexStatus();
    }

    public static boolean isValid(String status) {
        return status != null
                && (status.equals(READY)
                || status.equals(DELETED)
                || status.equals(NONE)
                || status.equals(INDEXING));
    }
}
