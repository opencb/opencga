package org.opencb.opencga.core.models.notification;

import java.util.List;

public class NotificationEmailConfiguration {

    private boolean active;
    private List<String> type;

    public NotificationEmailConfiguration() {
    }

    public NotificationEmailConfiguration(boolean active, List<String> type) {
        this.active = active;
        this.type = type;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationEmailConfiguration{");
        sb.append("active=").append(active);
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public boolean isActive() {
        return active;
    }

    public NotificationEmailConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }

    public List<String> getType() {
        return type;
    }

    public NotificationEmailConfiguration setType(List<String> type) {
        this.type = type;
        return this;
    }
}
