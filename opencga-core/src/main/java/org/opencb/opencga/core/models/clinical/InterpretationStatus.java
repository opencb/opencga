package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.common.Status;

import java.util.Arrays;
import java.util.List;

public class InterpretationStatus extends Status {

    public static final String NOT_REVIEWED = "NOT_REVIEWED";
    public static final String UNDER_REVIEW = "UNDER_REVIEW";
    public static final String REVIEWED = "REVIEWED";
    public static final String REJECTED = "REJECTED";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, NOT_REVIEWED, UNDER_REVIEW, REVIEWED, REJECTED);

    public InterpretationStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public InterpretationStatus() {
        this(NOT_REVIEWED, "");
    }

    public InterpretationStatus(String status) {
        this(status, "");
    }

    public static boolean isValid(String status) {
        if (Status.isValid(status)) {
            return true;
        }

        if (STATUS_LIST.contains(status)) {
            return true;
        }
        return false;
    }

}
