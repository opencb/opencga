package org.opencb.opencga.core.models.variant;


import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.Arrays;
import java.util.List;

public class OperationIndexStatus extends IndexStatus {

    public static final String PENDING = "PENDING";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, PENDING, NONE);

    public OperationIndexStatus(String status, String message) {
        if (isValid(status)) {
            init(status, status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public OperationIndexStatus(String status) {
        this(status, "");
    }

    public OperationIndexStatus() {
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

    public static OperationIndexStatus init() {
        return new OperationIndexStatus();
    }

    public static boolean isValid(String status) {
        return status != null
                && (status.equals(READY)
                || status.equals(PENDING)
                || status.equals(NONE));
    }
}
