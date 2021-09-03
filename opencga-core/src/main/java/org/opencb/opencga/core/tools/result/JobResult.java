/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.tools.result;

import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;

import java.net.URI;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class JobResult {

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

    public JobResult() {
        executor = new ExecutorInfo();
        status = new Status();
        events = new LinkedList<>();
        externalFiles = new LinkedList<>();
        steps = new LinkedList<>();
        attributes = new ObjectMap();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobResult{");
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

    public JobResult setId(String id) {
        this.id = id;
        return this;
    }

    public ExecutorInfo getExecutor() {
        return executor;
    }

    public JobResult setExecutor(ExecutorInfo executor) {
        this.executor = executor;
        return this;
    }

    public Date getStart() {
        return start;
    }

    public JobResult setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public JobResult setEnd(Date end) {
        this.end = end;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public JobResult setStatus(Status status) {
        this.status = status;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public JobResult setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

    public List<URI> getExternalFiles() {
        return externalFiles;
    }

    public JobResult setExternalFiles(List<URI> externalFiles) {
        this.externalFiles = externalFiles;
        return this;
    }

    public List<ToolStep> getSteps() {
        return steps;
    }

    public JobResult setSteps(List<ToolStep> steps) {
        this.steps = steps;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public JobResult setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
