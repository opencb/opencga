package org.opencb.opencga.core.models.notification;

public class NotificationConfiguration {

    private boolean active;
    private NotificationEmailConfiguration email;
    private NotificationSlackConfiguration slack;

    public NotificationConfiguration() {
    }

    public NotificationConfiguration(boolean active, NotificationEmailConfiguration email, NotificationSlackConfiguration slack) {
        this.active = active;
        this.email = email;
        this.slack = slack;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationConfiguration{");
        sb.append("active=").append(active);
        sb.append(", email=").append(email);
        sb.append(", slack=").append(slack);
        sb.append('}');
        return sb.toString();
    }

    public boolean isActive() {
        return active;
    }

    public NotificationConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }

    public NotificationEmailConfiguration getEmail() {
        return email;
    }

    public NotificationConfiguration setEmail(NotificationEmailConfiguration email) {
        this.email = email;
        return this;
    }

    public NotificationSlackConfiguration getSlack() {
        return slack;
    }

    public NotificationConfiguration setSlack(NotificationSlackConfiguration slack) {
        this.slack = slack;
        return this;
    }
}
