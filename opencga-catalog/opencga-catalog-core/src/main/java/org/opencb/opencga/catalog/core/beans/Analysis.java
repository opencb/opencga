package org.opencb.opencga.catalog.core.beans;

import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Analysis {

    private int id;
    private String name;
    private String alias;
    private String date;
    private String description;

    private List<Job> jobs; // FIXME revise inclusion

    private Map<String, Object> attributes;


    public Analysis() {
    }

    public Analysis(int id, String name, String alias, String date, String description, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.date = date;
        this.description = description;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Analysis{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", alias='" + alias + '\'' +
                ", date='" + date + '\'' +
                ", description='" + description + '\'' +
                ", jobs=" + jobs +
                ", attributes=" + attributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

}
