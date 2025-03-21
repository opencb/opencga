package org.opencb.opencga.core.models.notification;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.common.Internal;

import java.util.List;

public class NotificationInternal extends Internal {

    @DataField(id = "status", description = FieldConstants.INTERNAL_STATUS_DESCRIPTION)
    protected NotificationStatus status;

    @DataField(id = "notifications", managed = true, immutable = true,
            description = FieldConstants.NOTIFICATION_INTERNAL_NOTIFICATIONS_DESCRIPTION)
    private List<NotificationInternalNotificationResult> notifications;

    public NotificationInternal() {
    }

    public NotificationInternal(NotificationStatus status, String registrationDate, String lastModified,
                                List<NotificationInternalNotificationResult> notifications) {
        super(status, registrationDate, lastModified);
        this.notifications = notifications;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationInternal{");
        sb.append("notifications=").append(notifications);
        sb.append(", status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public NotificationInternal setRegistrationDate(String registrationDate) {
        super.setRegistrationDate(registrationDate);
        return this;
    }

    @Override
    public NotificationInternal setLastModified(String lastModified) {
        super.setLastModified(lastModified);
        return this;
    }

    @Override
    public NotificationStatus getStatus() {
        return status;
    }

    public NotificationInternal setStatus(NotificationStatus status) {
        this.status = status;
        return this;
    }

    public List<NotificationInternalNotificationResult> getNotifications() {
        return notifications;
    }

    public NotificationInternal setNotifications(List<NotificationInternalNotificationResult> notifications) {
        this.notifications = notifications;
        return this;
    }
}
