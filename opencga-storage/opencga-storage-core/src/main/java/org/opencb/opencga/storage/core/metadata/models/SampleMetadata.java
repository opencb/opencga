package org.opencb.opencga.storage.core.metadata.models;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleMetadata {

    private int studyId;
    private int id;
    private String name;
    private List<Integer> files;
    private List<Integer> cohorts;

    public SampleMetadata(int studyId) {
        this.studyId = studyId;
    }

    public int getId() {
        return id;
    }

    public SampleMetadata setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public SampleMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public List<Integer> getFiles() {
        return files;
    }

    public SampleMetadata setFiles(List<Integer> files) {
        this.files = files;
        return this;
    }

    public List<Integer> getCohorts() {
        return cohorts;
    }

    public SampleMetadata setCohorts(List<Integer> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

}
