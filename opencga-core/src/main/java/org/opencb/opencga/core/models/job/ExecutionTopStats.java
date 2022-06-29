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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ExecutionTopStats {

    private int processed;
    private int running;
    private int queued;
    private int blocked;
    private int pending;
    private int done;
    private int aborted;
    private int error;

    public ExecutionTopStats() {
    }

    public ExecutionTopStats(int processed, int blocked, int running, int queued, int pending, int done, int aborted, int error) {
        this.processed = processed;
        this.blocked = blocked;
        this.running = running;
        this.queued = queued;
        this.pending = pending;
        this.done = done;
        this.aborted = aborted;
        this.error = error;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionTopStats{");
        sb.append("processed=").append(processed);
        sb.append(", running=").append(running);
        sb.append(", queued=").append(queued);
        sb.append(", blocked=").append(blocked);
        sb.append(", pending=").append(pending);
        sb.append(", done=").append(done);
        sb.append(", aborted=").append(aborted);
        sb.append(", error=").append(error);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ExecutionTopStats that = (ExecutionTopStats) o;

        return new EqualsBuilder()
                .append(processed, that.processed)
                .append(running, that.running)
                .append(queued, that.queued)
                .append(blocked, that.blocked)
                .append(pending, that.pending)
                .append(done, that.done)
                .append(aborted, that.aborted)
                .append(error, that.error)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(processed)
                .append(running)
                .append(queued)
                .append(blocked)
                .append(pending)
                .append(done)
                .append(aborted)
                .append(error)
                .toHashCode();
    }

    public int getProcessed() {
        return processed;
    }

    public ExecutionTopStats setProcessed(int processed) {
        this.processed = processed;
        return this;
    }

    public int getRunning() {
        return running;
    }

    public ExecutionTopStats setRunning(int running) {
        this.running = running;
        return this;
    }

    public int getQueued() {
        return queued;
    }

    public ExecutionTopStats setQueued(int queued) {
        this.queued = queued;
        return this;
    }

    public int getBlocked() {
        return blocked;
    }

    public ExecutionTopStats setBlocked(int blocked) {
        this.blocked = blocked;
        return this;
    }

    public int getPending() {
        return pending;
    }

    public ExecutionTopStats setPending(int pending) {
        this.pending = pending;
        return this;
    }

    public int getDone() {
        return done;
    }

    public ExecutionTopStats setDone(int done) {
        this.done = done;
        return this;
    }

    public int getAborted() {
        return aborted;
    }

    public ExecutionTopStats setAborted(int aborted) {
        this.aborted = aborted;
        return this;
    }

    public int getError() {
        return error;
    }

    public ExecutionTopStats setError(int error) {
        this.error = error;
        return this;
    }
}
