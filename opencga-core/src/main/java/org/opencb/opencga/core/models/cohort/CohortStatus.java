package org.opencb.opencga.core.models.cohort;

import org.opencb.opencga.core.models.common.Status;

import java.util.Arrays;
import java.util.List;

public class CohortStatus extends Status {

    public static final String NONE = "NONE";
    public static final String CALCULATING = "CALCULATING";
    public static final String INVALID = "INVALID";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, NONE, CALCULATING, INVALID);

    public CohortStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public CohortStatus(String status) {
        this(status, "");
    }

    public CohortStatus() {
        this(NONE, "");
    }

    public static boolean isValid(String status) {
        if (Status.isValid(status)) {
            return true;
        }
        if (status != null && (status.equals(NONE) || status.equals(CALCULATING) || status.equals(INVALID))) {
            return true;
        }
        return false;
    }
}
