package org.opencb.opencga.catalog.beans;

import java.util.List;

/**
 * Created by imedina on 24/11/14.
 */
public class Dataset {

    private int id;
    private String name;
    private String creationDate;
    private String description;

    private List<File> files;


    public Dataset() {
    }

    public Dataset(int id, String name, String creationDate, String description, List<File> files) {
        this.id = id;
        this.name = name;
        this.creationDate = creationDate;
        this.description = description;
        this.files = files;
    }

    @Override
    public String toString() {
        return "Dataset{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", creationDate='" + creationDate + '\'' +
                ", description='" + description + '\'' +
                ", files=" + files +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public List<File> getFiles() {
        return files;
    }

    public void setFiles(List<File> files) {
        this.files = files;
    }

}
