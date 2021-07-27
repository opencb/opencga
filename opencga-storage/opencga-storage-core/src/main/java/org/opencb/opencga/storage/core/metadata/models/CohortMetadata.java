package org.opencb.opencga.storage.core.metadata.models;

import java.util.List;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CohortMetadata extends StudyResourceMetadata<CohortMetadata> {

//    private int studyId;
//    private int id;
//    private String name;

    private List<Integer> samples;
    private List<Integer> files;

//    private TaskMetadata.Status status = TaskMetadata.Status.NONE;

    public CohortMetadata() {
    }

    public CohortMetadata(int studyId, int id, String name, List<Integer> samples, List<Integer> files) {
        super(studyId, id, name);
        this.samples = samples;
        this.files = files;
    }

    public List<Integer> getSamples() {
        return samples;
    }

    public CohortMetadata setSamples(List<Integer> samples) {
        this.samples = samples;
        return this;
    }

    public List<Integer> getFiles() {
        return files;
    }

    public CohortMetadata setFiles(List<Integer> files) {
        this.files = files;
        return this;
    }

    public TaskMetadata.Status getStatsStatus() {
        return getStatus("stats");
    }

    public CohortMetadata setStatsStatus(TaskMetadata.Status status) {
        return setStatus("stats", status);
    }

    public boolean isStatsReady() {
        return isReady("stats");
    }

    public boolean isInvalid() {
        return isError("stats");
    }

    public CohortMetadata setInvalidStats() {
        setStatsStatus(TaskMetadata.Status.ERROR);
        return this;
    }

    public TaskMetadata.Status getSecondaryIndexStatus() {
        return getStatus("secondaryIndex");
    }

    public CohortMetadata setSecondaryIndexStatus(TaskMetadata.Status status) {
        return setStatus("secondaryIndex", status);
    }
}
