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

import java.util.Date;
import java.util.List;

public class JobTop {
    private Date date;
    private JobTopStats stats;
    private List<Job> jobs;
    
    public JobTop() {
    }

    public JobTop(Date date, JobTopStats stats, List<Job> jobs) {
        this.date = date;
        this.stats = stats;
        this.jobs = jobs;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobTop{");
        sb.append("date=").append(date);
        sb.append(", stats=").append(stats);
        sb.append(", jobs=").append(jobs);
        sb.append('}');
        return sb.toString();
    }

    public Date getDate() {
        return date;
    }

    public JobTop setDate(Date date) {
        this.date = date;
        return this;
    }

    public JobTopStats getStats() {
        return stats;
    }

    public JobTop setStats(JobTopStats stats) {
        this.stats = stats;
        return this;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public JobTop setJobs(List<Job> jobs) {
        this.jobs = jobs;
        return this;
    }

}
