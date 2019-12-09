package org.opencb.opencga.core.tools.result;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Date;

public class ToolStep {

    private String id;
    private Date start;
    private Date end;
    private Status.Type status;
    private ObjectMap attributes;

    public ToolStep() {
        attributes = new ObjectMap();
        status = null;
    }

    public ToolStep(String id, Date start, Date end, Status.Type status, ObjectMap attributes) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.status = status;
        this.attributes = attributes;
    }

    public String getId() {
        return id;
    }

    public ToolStep setId(String id) {
        this.id = id;
        return this;
    }

    public Date getStart() {
        return start;
    }

    public ToolStep setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public ToolStep setEnd(Date end) {
        this.end = end;
        return this;
    }

    public Status.Type getStatus() {
        return status;
    }

    public ToolStep setStatus(Status.Type status) {
        this.status = status;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public ToolStep setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
