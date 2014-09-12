package org.opencb.opencga.catalog.core.beans;

import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Analysis {

    private int id;
    private String name;
    private String alias;
    private String date;
    private int studyId;
    private String description;


    public Analysis() {
    }

    public Analysis(int id, String name, String alias, String date, int studyId, String description) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.date = date;
        this.studyId = studyId;
        this.description = description;
    }

    @Override
    public String toString() {
        return "Analysis{" +
                "description='" + description + '\'' +
                ", studyId=" + studyId +
                ", date='" + date + '\'' +
                ", alias='" + alias + '\'' +
                ", name='" + name + '\'' +
                ", id=" + id +
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

}
