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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.Date;

public class ToolStep {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "start", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_START)
    private Date start;

    @DataField(id = "end", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_END)
    private Date end;

    @DataField(id = "status", indexed = true,
            description = FieldConstants.EXECUTION_RESULT_STATUS)
    private Status.Type status;

    @DataField(id = "attributes", indexed = true,
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
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
