package org.opencb.opencga.core.models.study;

import java.net.URL;

public class StudyNotification {

    private URL webhook;

    public StudyNotification() {
    }

    public StudyNotification(URL webhook) {
        this.webhook = webhook;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyNotification{");
        sb.append("webhook=").append(webhook);
        sb.append('}');
        return sb.toString();
    }

    public URL getWebhook() {
        return webhook;
    }

    public StudyNotification setWebhook(URL webhook) {
        this.webhook = webhook;
        return this;
    }
}
