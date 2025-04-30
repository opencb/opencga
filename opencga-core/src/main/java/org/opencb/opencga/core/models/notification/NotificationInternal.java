package org.opencb.opencga.core.models.notification;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.common.Internal;

import java.util.List;

public class NotificationInternal extends Internal {

    @DataField(id = "status", description = FieldConstants.INTERNAL_STATUS_DESCRIPTION)
    protected NotificationStatus status;

    @DataField(id = "notificatorStatuses", managed = true, immutable = true,
            description = FieldConstants.NOTIFICATION_INTERNAL_NOTIFICATOR_STATUSES_DESCRIPTION)
    private List<NotificationInternalNotificationResult> notificatorStatuses;

    @DataField(id = "visited", description = FieldConstants.NOTIFICATION_INTERNAL_VISITED_DESCRIPTION)
    private boolean visited;

    public NotificationInternal() {
    }

    public NotificationInternal(NotificationStatus status, String registrationDate, String lastModified,
                                List<NotificationInternalNotificationResult> notificatorStatuses) {
        super(status, registrationDate, lastModified);
        this.notificatorStatuses = notificatorStatuses;
        this.visited = false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationInternal{");
        sb.append("notificatorStatuses=").append(notificatorStatuses);
        sb.append(", status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append(", visited='").append(visited).append('\'');
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

    public List<NotificationInternalNotificationResult> getNotificatorStatuses() {
        return notificatorStatuses;
    }

    public NotificationInternal setNotificatorStatuses(List<NotificationInternalNotificationResult> notificatorStatuses) {
        this.notificatorStatuses = notificatorStatuses;
        return this;
    }

    public boolean isVisited() {
        return visited;
    }

    public NotificationInternal setVisited(boolean visited) {
        this.visited = visited;
        return this;
    }
}
