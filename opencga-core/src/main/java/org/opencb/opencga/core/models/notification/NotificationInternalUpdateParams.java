package org.opencb.opencga.core.models.notification;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.List;

public class NotificationInternalUpdateParams {

    @DataField(id = "status", description = FieldConstants.INTERNAL_STATUS_DESCRIPTION)
    protected NotificationInternalStatusUpdateParams status;

    @DataField(id = "notificatorStatuses", managed = true, immutable = true,
            description = FieldConstants.NOTIFICATION_INTERNAL_NOTIFICATOR_STATUSES_DESCRIPTION)
    private List<NotificationInternalNotificationResult> notificatorStatuses;

    public NotificationInternalUpdateParams() {
    }

    public NotificationInternalUpdateParams(NotificationInternalStatusUpdateParams status,
                                            List<NotificationInternalNotificationResult> notificatorStatuses) {
        this.status = status;
        this.notificatorStatuses = notificatorStatuses;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationInternalUpdateParams{");
        sb.append("status=").append(status);
        sb.append(", notificatorStatuses=").append(notificatorStatuses);
        sb.append('}');
        return sb.toString();
    }

    public NotificationInternalStatusUpdateParams getStatus() {
        return status;
    }

    public NotificationInternalUpdateParams setStatus(NotificationInternalStatusUpdateParams status) {
        this.status = status;
        return this;
    }

    public List<NotificationInternalNotificationResult> getNotificatorStatuses() {
        return notificatorStatuses;
    }

    public NotificationInternalUpdateParams setNotificatorStatuses(List<NotificationInternalNotificationResult> notificatorStatuses) {
        this.notificatorStatuses = notificatorStatuses;
        return this;
    }

    public static class NotificationInternalStatusUpdateParams {

        private String id;
        private String description;

        public NotificationInternalStatusUpdateParams() {
        }

        public NotificationInternalStatusUpdateParams(String id, String description) {
            this.id = id;
            this.description = description;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("NotificationInternalStatusUpdateParams{");
            sb.append("id='").append(id).append('\'');
            sb.append(", description='").append(description).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public NotificationInternalStatusUpdateParams setId(String id) {
            this.id = id;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public NotificationInternalStatusUpdateParams setDescription(String description) {
            this.description = description;
            return this;
        }
    }
}
