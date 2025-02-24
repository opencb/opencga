package org.opencb.opencga.storage.core.metadata.models;

/**
 * Created on 14/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class StudyResourceMetadata<T extends StudyResourceMetadata<?>> extends ResourceMetadata<T> {

    private int studyId;

    StudyResourceMetadata() {
    }

    StudyResourceMetadata(int studyId, int id, String name) {
        super(id, name);
        this.studyId = studyId;
    }

    public int getStudyId() {
        return studyId;
    }

    public T setStudyId(int studyId) {
        this.studyId = studyId;
        return getThis();
    }

    @SuppressWarnings("unchecked")
    private T getThis() {
        return (T) this;
    }
}
