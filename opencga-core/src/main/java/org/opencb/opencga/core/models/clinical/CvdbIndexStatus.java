package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.common.OperationIndexStatus;

import java.util.Arrays;
import java.util.List;

public class CvdbIndexStatus extends OperationIndexStatus {

    public static final String ERROR = "ERROR";
    public static final String PENDING_INDEX = "PENDING_INDEX";
    public static final String PENDING_OVERWRITE = "PENDING_OVERWRITE";
    public static final String PENDING_REMOVE = "PENDING_REMOVE";
    public static final String QUEUED_INDEX = "QUEUED_INDEX";
    public static final String QUEUED_OVERWRITE = "QUEUED_OVERWRITE";
    public static final String QUEUED_REMOVE = "QUEUED_REMOVE";

    public static final List<String> STATUS_LIST = Arrays.asList(NONE, PENDING_INDEX, PENDING_REMOVE, PENDING_OVERWRITE, QUEUED_INDEX,
            QUEUED_REMOVE, QUEUED_OVERWRITE, READY, ERROR);

    public CvdbIndexStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public CvdbIndexStatus(String status) {
        this(status, "");
    }

    public CvdbIndexStatus() {
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

    public static CvdbIndexStatus init() {
        return new CvdbIndexStatus();
    }

    public static boolean isValid(String status) {
        return status != null
                && (status.equals(NONE)
                || status.equals(PENDING_INDEX)
                || status.equals(PENDING_REMOVE)
                || status.equals(PENDING_OVERWRITE)
                || status.equals(QUEUED_INDEX)
                || status.equals(QUEUED_REMOVE)
                || status.equals(QUEUED_OVERWRITE)
                || status.equals(READY)
                || status.equals(ERROR));
    }

}
