package org.opencb.opencga.catalog.beans;

import org.opencb.opencga.lib.common.TimeUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Analysis {

    private int id;
    private String name;
    private String alias;
    private String date;    //TODO: Necessary?
    private String creatorId;
    private String creationDate;
    private String description;

    private List<Job> jobs;

    private Map<String, Object> attributes;


    public Analysis() {
    }

    public Analysis(String name, String alias, String date, String creatorId, String description) {
        this(-1, name, alias, date, creatorId, TimeUtils.getTime(), description, new LinkedList<Job>(), new HashMap<String, Object>());
    }

    public Analysis(int id, String name, String alias, String date, String creatorId, String creationDate, String description) {
        this(id, name, alias, date, creatorId, creationDate, description, new LinkedList<Job>(), new HashMap<String, Object>());
    }
    public Analysis(int id, String name, String alias, String date, String creatorId, String creationDate,
                    String description, List<Job> jobs, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.date = date;
        this.creatorId = creatorId;
        this.creationDate = creationDate;
        this.description = description;
        this.jobs = jobs;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Analysis{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", alias='" + alias + '\'' +
                ", date='" + date + '\'' +
                ", creatorId='" + creatorId + '\'' +
                ", creationDate='" + creationDate + '\'' +
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

    public String getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(String creatorId) {
        this.creatorId = creatorId;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
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
