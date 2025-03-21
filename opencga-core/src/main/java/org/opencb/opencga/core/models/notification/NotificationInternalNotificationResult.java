package org.opencb.opencga.core.models.notification;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class NotificationInternalNotificationResult {

    public final static String EMAIL = "EMAIL";
    public final static String SLACK = "SLACK";

    public final static String SUCCESS = "SUCCESS";
    public final static String ERROR = "ERROR";

    @DataField(id = "id", immutable = true, description = FieldConstants.NOTIFICATION_INTERNAL_NOTIFICATIONS_ID_DESCRIPTION)
    private String id;

    @DataField(id = "status", immutable = true, description = FieldConstants.NOTIFICATION_INTERNAL_NOTIFICATIONS_STATUS_DESCRIPTION)
    private String status;

    @DataField(id = "errorMessage", immutable = true,
            description = FieldConstants.NOTIFICATION_INTERNAL_NOTIFICATIONS_ERROR_MESSAGE_DESCRIPTION)
    private String errorMessage;

    public NotificationInternalNotificationResult() {
    }

    public NotificationInternalNotificationResult(String id, String status) {
        this.id = id;
        this.status = status;
    }

    public NotificationInternalNotificationResult(String id, String status, String errorMessage) {
        this.id = id;
        this.status = status;
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationInternalNotificationResult{");
        sb.append("id='").append(id).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", errorMessage='").append(errorMessage).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public NotificationInternalNotificationResult setId(String id) {
        this.id = id;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public NotificationInternalNotificationResult setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public NotificationInternalNotificationResult setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }
}
