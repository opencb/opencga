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

package org.opencb.opencga.core.models.job;

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Internal;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.opencb.opencga.core.api.ParamConstants;

public class JobInternal extends Internal implements Cloneable {

    @DataField(id = "status", indexed = true,
            description = FieldConstants.JOB_INTERNAL_STATUS_DESCRIPTION)
    private Enums.ExecutionStatus status;

    @DataField(id = "webhook", indexed = true,
            description = FieldConstants.JOB_INTERNAL_WEBHOOK_DESCRIPTION)
    private JobInternalWebhook webhook;

    //TODO add tags to commons-datastore-core
    @DataField(id = "events", indexed = true, uncommentedClasses = {"Event"},
            description = FieldConstants.JOB_INTERNAL_EVENTS_DESCRIPTION)
    private List<Event> events;

    public JobInternal() {
    }

    public JobInternal(Enums.ExecutionStatus status) {
        this(null, null, status, null, null);
    }

    public JobInternal(String registrationDate, String modificationDate, Enums.ExecutionStatus status, JobInternalWebhook webhook,
                       List<Event> events) {
        super(null, registrationDate, modificationDate);
        this.status = status;
        this.webhook = webhook;
        this.events = events;
    }

    public static JobInternal init() {
        return new JobInternal(TimeUtils.getTime(), TimeUtils.getTime(), new Enums.ExecutionStatus(), null, new ArrayList<>());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobInternal{");
        sb.append("registrationDate='").append(registrationDate).append('\'');
        sb.append(", modificationDate='").append(lastModified).append('\'');
        sb.append(", status=").append(status);
        sb.append(", webhook=").append(webhook);
        sb.append(", events=").append(events);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public JobInternal clone() throws CloneNotSupportedException {
        return new JobInternal(registrationDate, lastModified, status, webhook.clone(), new LinkedList<>(events));
    }

    public Enums.ExecutionStatus getStatus() {
        return status;
    }

    public JobInternal setStatus(Enums.ExecutionStatus status) {
        this.status = status;
        return this;
    }

    public JobInternalWebhook getWebhook() {
        return webhook;
    }

    public JobInternal setWebhook(JobInternalWebhook webhook) {
        this.webhook = webhook;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public JobInternal setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public JobInternal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public JobInternal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
