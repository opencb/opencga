package org.opencb.opencga.storage.core.metadata.models;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleMetadata {

    private int studyId;
    private int id;
    private String name;
    private Set<Integer> files;
//    private Set<Integer> cohorts;
    private TaskMetadata.Status indexStatus;

    public SampleMetadata() {
        files = new HashSet<>();
//        cohorts = new HashSet<>();
        indexStatus = TaskMetadata.Status.NONE;
    }

    public SampleMetadata(int studyId, int id, String name) {
        this();
        this.studyId = studyId;
        this.id = id;
        this.name = name;
    }

    public int getStudyId() {
        return studyId;
    }

    public SampleMetadata setStudyId(int studyId) {
        this.studyId = studyId;
        return this;
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

    public Set<Integer> getFiles() {
        return files;
    }

    public SampleMetadata setFiles(Set<Integer> files) {
        this.files = files;
        return this;
    }

//    public Set<Integer> getCohorts() {
//        return cohorts;
//    }
//
//    public SampleMetadata setCohorts(Set<Integer> cohorts) {
//        this.cohorts = cohorts;
//        return this;
//    }


    public TaskMetadata.Status getIndexStatus() {
        return indexStatus;
    }

    public SampleMetadata setIndexStatus(TaskMetadata.Status indexStatus) {
        this.indexStatus = indexStatus;
        return this;
    }

    public boolean isIndexed() {
        return TaskMetadata.Status.READY.equals(this.indexStatus);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("studyId", studyId)
                .append("id", id)
                .append("name", name)
                .append("files", files)
//                .append("cohorts", cohorts)
                .toString();
    }
}
