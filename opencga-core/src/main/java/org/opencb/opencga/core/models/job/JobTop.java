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
