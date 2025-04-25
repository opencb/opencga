package org.opencb.opencga.core.models.notification;

import java.util.List;

public class NotificationEmailConfiguration extends AbstractNotificationScopeLevel {

    public NotificationEmailConfiguration() {
    }

    public NotificationEmailConfiguration(boolean active, NotificationLevel minLevel, List<NotificationScope> scopes) {
        super(active, minLevel, scopes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationEmailConfiguration{");
        sb.append("active=").append(active);
        sb.append(", minLevel='").append(minLevel).append('\'');
        sb.append(", scopes=").append(scopes);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public NotificationEmailConfiguration setActive(boolean active) {
        super.setActive(active);
        return this;
    }

    @Override
    public NotificationEmailConfiguration setMinLevel(NotificationLevel minLevel) {
        super.setMinLevel(minLevel);
        return this;
    }

    @Override
    public NotificationEmailConfiguration setScopes(List<NotificationScope> scopes) {
        super.setScopes(scopes);
        return this;
    }
}
