package org.opencb.opencga.master.tasks.result;

import org.opencb.opencga.core.analysis.result.Status;

import java.util.Date;

public class Step {

    private String id;
    private String description;
    private Date start;
    private Date end;
    private Status.Type status;

    public Step() {
    }

    public Step(String id, String description, Date start, Status.Type status) {
        this.id = id;
        this.description = description;
        this.start = start;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public Step setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Step setDescription(String description) {
        this.description = description;
        return this;
    }

    public Date getStart() {
        return start;
    }

    public Step setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public Step setEnd(Date end) {
        this.end = end;
        return this;
    }

    public Status.Type getStatus() {
        return status;
    }

    public Step setStatus(Status.Type status) {
        this.status = status;
        return this;
    }
}
