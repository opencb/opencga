package org.opencb.opencga.core.tools.result;

import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;

import java.net.URI;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ExecutionResult {

    @Deprecated
    private String id;
    private ExecutorInfo executor;
    private Date start;
    private Date end;
    private Status status;
    private List<URI> externalFiles;
    private List<ToolStep> steps;
    private List<Event> events;

    private ObjectMap attributes;

    public ExecutionResult() {
        executor = new ExecutorInfo();
        status = new Status();
        events = new LinkedList<>();
        externalFiles = new LinkedList<>();
        steps = new LinkedList<>();
        attributes = new ObjectMap();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionResult{");
        sb.append("executor=").append(executor);
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", status=").append(status);
        sb.append(", externalFiles=").append(externalFiles);
        sb.append(", steps=").append(steps);
        sb.append(", events=").append(events);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ExecutionResult setId(String id) {
        this.id = id;
        return this;
    }

    public ExecutorInfo getExecutor() {
        return executor;
    }

    public ExecutionResult setExecutor(ExecutorInfo executor) {
        this.executor = executor;
        return this;
    }

    public Date getStart() {
        return start;
    }

    public ExecutionResult setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public ExecutionResult setEnd(Date end) {
        this.end = end;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public ExecutionResult setStatus(Status status) {
        this.status = status;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public ExecutionResult setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

    public List<URI> getExternalFiles() {
        return externalFiles;
    }

    public ExecutionResult setExternalFiles(List<URI> externalFiles) {
        this.externalFiles = externalFiles;
        return this;
    }

    public List<ToolStep> getSteps() {
        return steps;
    }

    public ExecutionResult setSteps(List<ToolStep> steps) {
        this.steps = steps;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public ExecutionResult setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
