package org.opencb.opencga.storage.core.metadata.models;

import org.opencb.commons.datastore.core.ObjectMap;

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

    private ObjectMap attributes;

    StudyResourceMetadata() {
    }

    StudyResourceMetadata(int studyId, int id, String name) {
        this.studyId = studyId;
        this.id = id;
        this.name = name;
    }

    public int getStudyId() {
        return studyId;
    }

    public T setStudyId(int studyId) {
        this.studyId = studyId;
        return getThis();
    }

    public int getId() {
        return id;
    }

    public T setId(int id) {
        this.id = id;
        return getThis();
    }

    public String getName() {
        return name;
    }

    public T setName(String name) {
        this.name = name;
        return getThis();
    }

    public Map<String, TaskMetadata.Status> getStatus() {
        return status;
    }

    public TaskMetadata.Status getStatus(String statusName) {
        return getStatus(statusName, TaskMetadata.Status.NONE);
    }

    public TaskMetadata.Status getStatus(String statusName, TaskMetadata.Status defaultValue) {
        return getStatus().getOrDefault(statusName, defaultValue);
    }

    public T setStatus(String statusName, TaskMetadata.Status status) {
        getStatus().put(statusName, status);
        return getThis();
    }

    public boolean isReady(String statusName) {
        return TaskMetadata.Status.READY.equals(getStatus(statusName));
    }

    public boolean isError(String statusName) {
        return TaskMetadata.Status.ERROR.equals(getStatus(statusName));
    }

    public T setStatus(Map<String, TaskMetadata.Status> status) {
        this.status = status;
        return getThis();
    }

    public ObjectMap getAttributes() {
        if (attributes == null) {
            attributes = new ObjectMap();
        }
        return attributes;
    }

    public T setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return getThis();
    }

    @SuppressWarnings("unchecked")
    public T getThis() {
        return (T) this;
    }
}
