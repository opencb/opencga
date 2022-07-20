package org.opencb.opencga.core.models.job;

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class JobReferenceParam {

    @DataField(description = ParamConstants.JOB_REFERENCE_PARAM_STUDY_ID_DESCRIPTION)
    private String studyId;
    @DataField(description = ParamConstants.JOB_REFERENCE_PARAM_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.JOB_REFERENCE_PARAM_ID_DESCRIPTION)
    private String uuid;

    public JobReferenceParam() {
    }

    public JobReferenceParam(Job job) {
        this.studyId = job.getStudy().getId();
        this.id = job.getId();
        this.uuid = job.getUuid();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobReferenceParam{");
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
        JobReferenceParam that = (JobReferenceParam) o;
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

    public JobReferenceParam setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getId() {
        return id;
    }

    public JobReferenceParam setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public JobReferenceParam setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }
}
