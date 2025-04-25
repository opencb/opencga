package org.opencb.opencga.core.models.notification;

public class NotificationConfiguration {

    private NotificationEmailConfiguration email;
    private NotificationSlackConfiguration slack;

    public NotificationConfiguration() {
    }

    public NotificationConfiguration(NotificationEmailConfiguration email, NotificationSlackConfiguration slack) {
        this.email = email;
        this.slack = slack;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationConfiguration{");
        sb.append(", email=").append(email);
        sb.append(", slack=").append(slack);
        sb.append('}');
        return sb.toString();
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
