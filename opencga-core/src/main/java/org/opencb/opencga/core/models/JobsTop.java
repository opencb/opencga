package org.opencb.opencga.core.models;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class JobsTop {
    private Date date;
    private Map<String, Long> jobStatusCount;
    private List<Job> jobs;
    
    public JobsTop() {
    }

    public JobsTop(Date date, Map<String, Long> jobStatusCount, List<Job> jobs) {
        this.date = date;
        this.jobStatusCount = jobStatusCount;
        this.jobs = jobs;
    }

    public Date getDate() {
        return date;
    }

    public JobsTop setDate(Date date) {
        this.date = date;
        return this;
    }

    public Map<String, Long> getJobStatusCount() {
        return jobStatusCount;
    }

    public JobsTop setJobStatusCount(Map<String, Long> jobStatusCount) {
        this.jobStatusCount = jobStatusCount;
        return this;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public JobsTop setJobs(List<Job> jobs) {
        this.jobs = jobs;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobsTop{");
        sb.append("date=").append(date);
        sb.append(", jobStatusCount=").append(jobStatusCount);
        sb.append(", jobs=").append(jobs);
        sb.append('}');
        return sb.toString();
    }
}
