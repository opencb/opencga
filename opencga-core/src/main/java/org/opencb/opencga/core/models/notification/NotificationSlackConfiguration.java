package org.opencb.opencga.core.models.notification;

import java.util.List;

public class NotificationSlackConfiguration extends AbstractNotificationScopeLevel {

    private String webhookUrl;

    public NotificationSlackConfiguration() {
    }

    public NotificationSlackConfiguration(boolean active, String webhookUrl, NotificationLevel minLevel, List<NotificationScope> scopes) {
        this.active = active;
        this.webhookUrl = webhookUrl;
        this.minLevel = minLevel;
        this.scopes = scopes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationSlackConfiguration{");
        sb.append("active=").append(active);
        sb.append(", webhookUrl='").append(webhookUrl).append('\'');
        sb.append(", minLevel='").append(minLevel).append('\'');
        sb.append(", scopes=").append(scopes);
        sb.append('}');
        return sb.toString();
    }

    public String getWebhookUrl() {
        return webhookUrl;
    }

    public NotificationSlackConfiguration setWebhookUrl(String webhookUrl) {
        this.webhookUrl = webhookUrl;
        return this;
    }

    @Override
    public NotificationSlackConfiguration setActive(boolean active) {
        super.setActive(active);
        return this;
    }

    @Override
    public NotificationSlackConfiguration setMinLevel(NotificationLevel minLevel) {
        super.setMinLevel(minLevel);
        return this;
    }

    @Override
    public NotificationSlackConfiguration setScopes(List<NotificationScope> scopes) {
        super.setScopes(scopes);
        return this;
    }
}
