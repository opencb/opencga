package org.opencb.opencga.catalog.core.beans;

import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Analysis {

    //private int id;
    private String name;
    private String date;
    private int studyId;
    private String description;

    private List<Job> jobs;

    public Analysis() {
    }

    public Analysis(String name, String date, int studyId, String description, List<Job> jobs) {
        this.name = name;
        this.date = date;
        this.studyId = studyId;
        this.description = description;
        this.jobs = jobs;
    }

    @Override
    public String toString() {
        return "Analysis{" +
                "name='" + name + '\'' +
                ", date='" + date + '\'' +
                ", studyId=" + studyId +
                ", description='" + description + '\'' +
                ", jobs=" + jobs +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getStudyId() {
        return studyId;
    }

    public void setStudyId(int studyId) {
        this.studyId = studyId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public void setJobs(List<Job> jobs) {
        this.jobs = jobs;
    }
}
