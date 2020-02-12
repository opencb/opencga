package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.models.common.Enums;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class JobInternalWebhook {

    private URL webhook;
    private Map<Enums.ExecutionStatus, Status> status;

    public JobInternalWebhook() {
        this(null, new HashMap<>());
    }

    public JobInternalWebhook(URL webhook, Map<Enums.ExecutionStatus, Status> status) {
        this.webhook = webhook;
        this.status = status != null ? status : new HashMap<>();
    }

    public enum Status {
        SUCCESS,
        ERROR
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobInternalWebhook{");
        sb.append("webhook=").append(webhook);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public URL getWebhook() {
        return webhook;
    }

    public JobInternalWebhook setWebhook(URL webhook) {
        this.webhook = webhook;
        return this;
    }

    public Map<Enums.ExecutionStatus, Status> getStatus() {
        return status;
    }

    public JobInternalWebhook setStatus(Map<Enums.ExecutionStatus, Status> status) {
        this.status = status;
        return this;
    }
}
