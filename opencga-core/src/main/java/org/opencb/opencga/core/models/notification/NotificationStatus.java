package org.opencb.opencga.core.models.notification;

import org.opencb.opencga.core.models.common.InternalStatus;

import java.util.Arrays;
import java.util.List;

public class NotificationStatus extends InternalStatus {

    public static final String PENDING = "PENDING";
    public static final String SUCCESS = "SUCCESS";
    public static final String ERROR = "ERROR";
    public static final String DISCARDED = "DISCARDED";

    public static final List<String> STATUS_LIST = Arrays.asList(PENDING, SUCCESS, ERROR, DISCARDED);

    public NotificationStatus() {
    }

    public NotificationStatus(String id, String description) {
        if (isValid(id)) {
            init(id, description);
        } else {
            throw new IllegalArgumentException("Unknown status id '" + id + "'");
        }
    }

    public static boolean isValid(String statusId) {
        return statusId != null && STATUS_LIST.contains(statusId);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationStatus{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", commit='").append(commit).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
