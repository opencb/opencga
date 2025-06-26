package org.opencb.opencga.core.models.common;

import java.util.Arrays;
import java.util.List;

public class QualityControlStatus extends InternalStatus {

    public static final String ERROR = "ERROR";
    public static final String PENDING = "PENDING";
    public static final String PENDING_REMOVE = "PENDING_REMOVE";
    public static final String PENDING_OVERWRITE = "PENDING_OVERWRITE";

    public static final List<String> STATUS_LIST = Arrays.asList(PENDING, PENDING_REMOVE, PENDING_OVERWRITE, READY, ERROR);

    public QualityControlStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public QualityControlStatus(String status) {
        this(status, "");
    }

    public QualityControlStatus() {
        this(PENDING, "");
    }

    public static QualityControlStatus init() {
        return new QualityControlStatus();
    }

    public static boolean isValid(String status) {
        return status != null
                && (status.equals(PENDING)
                || status.equals(PENDING_REMOVE)
                || status.equals(PENDING_OVERWRITE)
                || status.equals(READY)
                || status.equals(ERROR));
    }

}
