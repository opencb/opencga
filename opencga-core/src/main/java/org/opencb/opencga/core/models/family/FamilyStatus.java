package org.opencb.opencga.core.models.family;

import org.opencb.opencga.core.models.common.Status;

import java.util.Arrays;
import java.util.List;

public class FamilyStatus extends Status {

    public static final String INCOMPLETE = "INCOMPLETE";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, INCOMPLETE);

    public FamilyStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public FamilyStatus(String status) {
        this(status, "");
    }

    public FamilyStatus() {
        this(READY, "");
    }

    public static boolean isValid(String status) {
        if (Status.isValid(status)) {
            return true;
        }
        if (status != null && (status.equals(INCOMPLETE))) {
            return true;
        }
        return false;
    }
}
