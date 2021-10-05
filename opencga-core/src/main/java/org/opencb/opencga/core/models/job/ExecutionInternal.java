package org.opencb.opencga.core.models.job;

import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Internal;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ExecutionInternal extends Internal implements Cloneable {

    private Enums.ExecutionStatus status;
    private JobInternalWebhook webhook;
    private List<Event> events;

    public ExecutionInternal() {
    }

    public ExecutionInternal(Enums.ExecutionStatus status) {
        this(null, null, status, null, null);
    }

    public ExecutionInternal(String registrationDate, String lastModified, Enums.ExecutionStatus status, JobInternalWebhook webhook,
                             List<Event> events) {
        super(null, registrationDate, lastModified);
        this.status = status;
        this.webhook = webhook;
        this.events = events;
    }

    public static ExecutionInternal init() {
        return new ExecutionInternal(TimeUtils.getTime(), TimeUtils.getTime(), new Enums.ExecutionStatus(), null, new ArrayList<>());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionInternal{");
        sb.append("registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append(", status=").append(status);
        sb.append(", webhook=").append(webhook);
        sb.append(", events=").append(events);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public ExecutionInternal clone() throws CloneNotSupportedException {
        return new ExecutionInternal(registrationDate, lastModified, status, webhook.clone(), new LinkedList<>(events));
    }

    public Enums.ExecutionStatus getStatus() {
        return status;
    }

    public ExecutionInternal setStatus(Enums.ExecutionStatus status) {
        this.status = status;
        return this;
    }

    public JobInternalWebhook getWebhook() {
        return webhook;
    }

    public ExecutionInternal setWebhook(JobInternalWebhook webhook) {
        this.webhook = webhook;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public ExecutionInternal setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public ExecutionInternal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public ExecutionInternal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
