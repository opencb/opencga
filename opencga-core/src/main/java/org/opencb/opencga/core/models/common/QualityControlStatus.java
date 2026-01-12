package org.opencb.opencga.core.models.common;

import java.util.Arrays;
import java.util.List;

public class QualityControlStatus extends InternalStatus {

    public static final String ERROR = "ERROR";
    public static final String NONE = "NONE";
    public static final String PENDING = "PENDING";
    public static final String QUEUED = "QUEUED";

    public static final List<String> STATUS_LIST = Arrays.asList(NONE, PENDING, QUEUED, READY, ERROR);

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
        this(NONE, "");
    }

    public static QualityControlStatus init() {
        return new QualityControlStatus();
    }

    public static boolean isValid(String status) {
        return status != null
                && (status.equals(NONE)
                || status.equals(PENDING)
                || status.equals(QUEUED)
                || status.equals(READY)
                || status.equals(ERROR));
    }

}
