package org.opencb.opencga.storage.core.metadata.models;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 14/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class StudyResourceMetadata<T extends StudyResourceMetadata> {

    private int studyId;
    private int id;
    private String name;

    private Map<String, TaskMetadata.Status> status = new HashMap<>();

    public StudyResourceMetadata() {
    }

    public StudyResourceMetadata(int studyId, int id, String name) {
        this.studyId = studyId;
        this.id = id;
        this.name = name;
    }

    public int getStudyId() {
        return studyId;
    }

    public T setStudyId(int studyId) {
        this.studyId = studyId;
        return (T) this;
    }

    public int getId() {
        return id;
    }

    public T setId(int id) {
        this.id = id;
        return (T) this;
    }

    public String getName() {
        return name;
    }

    public T setName(String name) {
        this.name = name;
        return (T) this;
    }

    public Map<String, TaskMetadata.Status> getStatus() {
        return status;
    }

    public TaskMetadata.Status getStatus(String statusName) {
        return getStatus().getOrDefault(statusName, TaskMetadata.Status.NONE);
    }

    public T setStatus(String statusName, TaskMetadata.Status status) {
        getStatus().put(statusName, status);
        return (T) this;
    }

    public boolean isReady(String statusName) {
        return TaskMetadata.Status.READY.equals(getStatus(statusName));
    }

    public boolean isError(String statusName) {
        return TaskMetadata.Status.READY.equals(getStatus(statusName));
    }

    public T setStatus(Map<String, TaskMetadata.Status> status) {
        this.status = status;
        return (T) this;
    }

}
