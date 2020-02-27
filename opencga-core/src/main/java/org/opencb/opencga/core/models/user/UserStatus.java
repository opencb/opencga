package org.opencb.opencga.core.models.user;

import org.opencb.opencga.core.models.common.Status;

import java.util.Arrays;
import java.util.List;

public class UserStatus extends Status {

    public static final String BANNED = "BANNED";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, BANNED);

    public UserStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public UserStatus(String status) {
        this(status, "");
    }

    public UserStatus() {
        this(READY, "");
    }

    public static boolean isValid(String status) {
        if (Status.isValid(status)) {
            return true;
        }
        if (status != null && (status.equals(BANNED))) {
            return true;
        }
        return false;
    }
}
