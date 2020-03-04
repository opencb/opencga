package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.Status;

import java.util.Arrays;
import java.util.List;

public class FileStatus extends Status {

    /**
     * TRASHED name means that the object is marked as deleted although is still available in the database.
     */
    public static final String TRASHED = "TRASHED";

    public static final String STAGE = "STAGE";
    public static final String MISSING = "MISSING";
    public static final String PENDING_DELETE = "PENDING_DELETE";
    public static final String DELETING = "DELETING"; // This status is set exactly before deleting the file from disk.
    public static final String REMOVED = "REMOVED";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, TRASHED, STAGE, MISSING, PENDING_DELETE, DELETING,
            REMOVED);

    public FileStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public FileStatus(String status) {
        this(status, "");
    }

    public FileStatus() {
        this(READY, "");
    }

    public static boolean isValid(String status) {
        if (Status.isValid(status)) {
            return true;
        }
        if (status != null && (status.equals(STAGE) || status.equals(MISSING) || status.equals(TRASHED)
                || status.equals(PENDING_DELETE) || status.equals(DELETING) || status.equals(REMOVED))) {
            return true;
        }
        return false;
    }
}
