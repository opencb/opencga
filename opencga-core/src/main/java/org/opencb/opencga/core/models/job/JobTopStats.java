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

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class JobTopStats {

    @DataField(description = ParamConstants.JOB_TOP_STATS_RUNNING_DESCRIPTION)
    private int running;
    @DataField(description = ParamConstants.JOB_TOP_STATS_QUEUED_DESCRIPTION)
    private int queued;
    @DataField(description = ParamConstants.JOB_TOP_STATS_PENDING_DESCRIPTION)
    private int pending;
    @DataField(description = ParamConstants.JOB_TOP_STATS_DONE_DESCRIPTION)
    private int done;
    @DataField(description = ParamConstants.JOB_TOP_STATS_ABORTED_DESCRIPTION)
    private int aborted;
    @DataField(description = ParamConstants.JOB_TOP_STATS_ERROR_DESCRIPTION)
    private int error;

    public JobTopStats() {
    }

    public JobTopStats(int running, int queued, int pending, int done, int aborted, int error) {
        this.running = running;
        this.queued = queued;
        this.pending = pending;
        this.done = done;
        this.aborted = aborted;
        this.error = error;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobTopStats{");
        sb.append("running=").append(running);
        sb.append(", queued=").append(queued);
        sb.append(", pending=").append(pending);
        sb.append(", done=").append(done);
        sb.append(", aborted=").append(aborted);
        sb.append(", error=").append(error);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobTopStats that = (JobTopStats) o;
        return running == that.running
                && queued == that.queued
                && pending == that.pending
                && done == that.done
                && aborted == that.aborted
                && error == that.error;
    }

    @Override
    public int hashCode() {
        return Objects.hash(running, queued, pending, done, aborted, error);
    }

    public int getRunning() {
        return running;
    }

    public JobTopStats setRunning(int running) {
        this.running = running;
        return this;
    }

    public int getQueued() {
        return queued;
    }

    public JobTopStats setQueued(int queued) {
        this.queued = queued;
        return this;
    }

    public int getPending() {
        return pending;
    }

    public JobTopStats setPending(int pending) {
        this.pending = pending;
        return this;
    }

    public int getDone() {
        return done;
    }

    public JobTopStats setDone(int done) {
        this.done = done;
        return this;
    }

    public int getAborted() {
        return aborted;
    }

    public JobTopStats setAborted(int aborted) {
        this.aborted = aborted;
        return this;
    }

    public int getError() {
        return error;
    }

    public JobTopStats setError(int error) {
        this.error = error;
        return this;
    }
}
