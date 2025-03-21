package org.opencb.opencga.core.models.notification;

import java.util.List;

public class NotificationSlackConfiguration {

    private boolean active;
    private String webhookUrl;
    private List<String> type;

    public NotificationSlackConfiguration() {
    }

    public NotificationSlackConfiguration(boolean active, String webhookUrl, List<String> type) {
        this.active = active;
        this.webhookUrl = webhookUrl;
        this.type = type;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationSlackConfiguration{");
        sb.append("active=").append(active);
        sb.append(", webhookUrl='").append(webhookUrl).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public boolean isActive() {
        return active;
    }

    public NotificationSlackConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getWebhookUrl() {
        return webhookUrl;
    }

    public NotificationSlackConfiguration setWebhookUrl(String webhookUrl) {
        this.webhookUrl = webhookUrl;
        return this;
    }

    public List<String> getType() {
        return type;
    }

    public NotificationSlackConfiguration setType(List<String> type) {
        this.type = type;
        return this;
    }
}
