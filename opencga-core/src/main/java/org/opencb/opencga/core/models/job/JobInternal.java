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

import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.models.common.Enums;

import java.util.LinkedList;
import java.util.List;

public class JobInternal implements Cloneable {

    private Enums.ExecutionStatus status;
    private JobInternalWebhook webhook;
    private List<Event> events;

    public JobInternal() {
    }

    public JobInternal(Enums.ExecutionStatus status) {
        this(status, null, null);
    }

    public JobInternal(Enums.ExecutionStatus status, JobInternalWebhook webhook, List<Event> events) {
        this.status = status;
        this.webhook = webhook;
        this.events = events;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobInternal{");
        sb.append("status=").append(status);
        sb.append(", webhook=").append(webhook);
        sb.append(", events=").append(events);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public JobInternal clone() throws CloneNotSupportedException {
        return new JobInternal(status, webhook.clone(), new LinkedList<>(events));
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
}
