package org.opencb.opencga.core.models.job;

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Internal;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ExecutionInternal extends Internal implements Cloneable {

    private String toolId;
    private Enums.ExecutionStatus status;
    private JobInternalWebhook webhook;
    private List<Event> events;

    @DataField(id = "start", indexed = true,
            description = FieldConstants.EXECUTION_INTERNAL_START)
    private Date start;

    @DataField(id = "end", indexed = true,
            description = FieldConstants.EXECUTION_INTERNAL_START)
    private Date end;

    public ExecutionInternal() {
    }

    public ExecutionInternal(Enums.ExecutionStatus status) {
        this(null, null, null, status, null, null, null, null);
    }

    public ExecutionInternal(String toolId, String registrationDate, String lastModified, Enums.ExecutionStatus status,
                             JobInternalWebhook webhook, List<Event> events, Date start, Date end) {
        super(null, registrationDate, lastModified);
        this.toolId = toolId;
        this.status = status;
        this.webhook = webhook;
        this.events = events;
        this.start = start;
        this.end = end;
    }

    public static ExecutionInternal init() {
        return new ExecutionInternal(null, TimeUtils.getTime(), TimeUtils.getTime(), new Enums.ExecutionStatus(), null, new ArrayList<>(),
                null, null);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionInternal{");
        sb.append("toolId='").append(toolId).append('\'');
        sb.append(", status=").append(status);
        sb.append(", webhook=").append(webhook);
        sb.append(", events=").append(events);
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public ExecutionInternal clone() throws CloneNotSupportedException {
        return new ExecutionInternal(toolId, registrationDate, lastModified, status, webhook.clone(), new LinkedList<>(events), start, end);
    }

    public String getToolId() {
        return toolId;
    }

    public ExecutionInternal setToolId(String toolId) {
        this.toolId = toolId;
        return this;
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

    public Date getStart() {
        return start;
    }

    public ExecutionInternal setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public ExecutionInternal setEnd(Date end) {
        this.end = end;
        return this;
    }
}
