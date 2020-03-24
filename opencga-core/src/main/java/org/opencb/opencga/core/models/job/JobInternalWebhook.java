package org.opencb.opencga.core.models.job;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class JobInternalWebhook implements Cloneable {

    private URL url;
    private Map<String, Status> status;

    public JobInternalWebhook() {
    }

    public JobInternalWebhook(URL url, Map<String, Status> status) {
        this.url = url;
        this.status = status;
    }

    public enum Status {
        SUCCESS,
        ERROR
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobInternalWebhook{");
        sb.append("webhook=").append(url);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public JobInternalWebhook clone() throws CloneNotSupportedException {
        JobInternalWebhook jobInternalWebhook = (JobInternalWebhook) super.clone();
        jobInternalWebhook.setStatus(new HashMap<>(status));
        return jobInternalWebhook;
    }

    public URL getUrl() {
        return url;
    }

    public JobInternalWebhook setUrl(URL url) {
        this.url = url;
        return this;
    }

    public Map<String, Status> getStatus() {
        return status;
    }

    public JobInternalWebhook setStatus(Map<String, Status> status) {
        this.status = status;
        return this;
    }
}
