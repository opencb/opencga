package org.opencb.opencga.core.models.notification;

import java.util.List;

public abstract class AbstractNotificationScopeLevel {

    protected boolean active;
    protected NotificationLevel minLevel;
    protected List<NotificationScope> scopes;

    public AbstractNotificationScopeLevel() {
    }

    public AbstractNotificationScopeLevel(boolean active, NotificationLevel minLevel, List<NotificationScope> scopes) {
        this.active = active;
        this.minLevel = minLevel;
        this.scopes = scopes;
    }

    public boolean isActive() {
        return active;
    }

    public AbstractNotificationScopeLevel setActive(boolean active) {
        this.active = active;
        return this;
    }

    public NotificationLevel getMinLevel() {
        return minLevel;
    }

    public AbstractNotificationScopeLevel setMinLevel(NotificationLevel minLevel) {
        this.minLevel = minLevel;
        return this;
    }

    public List<NotificationScope> getScopes() {
        return scopes;
    }

    public AbstractNotificationScopeLevel setScopes(List<NotificationScope> scopes) {
        this.scopes = scopes;
        return this;
    }
}
