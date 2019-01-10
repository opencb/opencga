package org.opencb.opencga.storage.core.metadata.models;

import java.util.List;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CohortMetadata {

    private int studyId;
    private int id;
    private String name;

    private List<Integer> samples;

    private BatchFileTask.Status status;

    public int getStudyId() {
        return studyId;
    }

    public CohortMetadata setStudyId(int studyId) {
        this.studyId = studyId;
        return this;
    }

    public int getId() {
        return id;
    }

    public CohortMetadata setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public CohortMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public List<Integer> getSamples() {
        return samples;
    }

    public CohortMetadata setSamples(List<Integer> samples) {
        this.samples = samples;
        return this;
    }

    public BatchFileTask.Status getStatus() {
        return status;
    }

    public CohortMetadata setStatus(BatchFileTask.Status status) {
        this.status = status;
        return this;
    }
}
