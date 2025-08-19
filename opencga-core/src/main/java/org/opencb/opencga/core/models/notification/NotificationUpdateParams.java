package org.opencb.opencga.core.models.notification;

public class NotificationUpdateParams {

    private NotificationInternalUpdateParams internal;

    public NotificationUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationUpdateParams{");
        sb.append("internal=").append(internal);
        sb.append('}');
        return sb.toString();
    }

    public NotificationInternalUpdateParams getInternal() {
        return internal;
    }

    public NotificationUpdateParams setInternal(NotificationInternalUpdateParams internal) {
        this.internal = internal;
        return this;
    }
}
