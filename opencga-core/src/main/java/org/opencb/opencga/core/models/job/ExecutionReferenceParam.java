package org.opencb.opencga.core.models.job;

import java.util.Objects;

public class ExecutionReferenceParam {

    private String studyId;
    private String id;
    private String uuid;

    public ExecutionReferenceParam() {
    }

    public ExecutionReferenceParam(Execution execution) {
        this.studyId = execution.getStudy().getId();
        this.id = execution.getId();
        this.uuid = execution.getUuid();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionReferenceParam{");
        sb.append("studyFqn='").append(studyId).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExecutionReferenceParam that = (ExecutionReferenceParam) o;
        return Objects.equals(studyId, that.studyId)
                && Objects.equals(id, that.id)
                && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(studyId, id, uuid);
    }

    public String getStudyId() {
        return studyId;
    }

    public ExecutionReferenceParam setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getId() {
        return id;
    }

    public ExecutionReferenceParam setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public ExecutionReferenceParam setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }
}
