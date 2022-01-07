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

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;

import java.net.URI;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ExecutionResult {

    @Deprecated
    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;


    @DataField(id = "executor", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_EXECUTION_INFO)
    private ExecutorInfo executor;

    @DataField(id = "start", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_START)
    private Date start;

    @DataField(id = "end", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_END)
    private Date end;

    @DataField(id = "status", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_STATUS)
    private Status status;


    @DataField(id = "externalFiles", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_EXTERNAL_FILES)
    private List<URI> externalFiles;

    @DataField(id = "steps", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_STEPS)
    private List<ToolStep> steps;

    @DataField(id = "events", indexed = true, uncommentedClasses = {"Event"},
            description = FieldConstants.EXECUTION_RESULT_EVENTS)
    private List<Event> events;

    @DataField(id = "attributes", indexed = true,
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
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
