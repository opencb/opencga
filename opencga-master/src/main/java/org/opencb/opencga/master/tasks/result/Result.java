package org.opencb.opencga.master.tasks.result;

import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.analysis.result.Status;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class Result {

    private Date start;
    private Date end;
    private Status status;
    private List<Step> steps;
    private List<Event> events;

    public Result() {
        status = new Status();
        steps = new LinkedList<>();
        events = new LinkedList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Result{");
        sb.append("start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", status=").append(status);
        sb.append(", steps=").append(steps);
        sb.append(", events=").append(events);
        sb.append('}');
        return sb.toString();
    }

    public Date getStart() {
        return start;
    }

    public Result setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public Result setEnd(Date end) {
        this.end = end;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Result setStatus(Status status) {
        this.status = status;
        return this;
    }

    public List<Step> getSteps() {
        return steps;
    }

    public Result setSteps(List<Step> steps) {
        this.steps = steps;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public Result setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

}
