package org.opencb.opencga.core.events;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.response.OpenCGAResult;

public class OpencgaEvent {

    private String eventId;
    private ObjectMap inputParams;

    private String organizationId;
    private String resourceId;
    private String resourceUuid;
    private String studyFqn;
    private String studyUuid;

    private String userId;
    private String token;
    private OpenCGAResult<?> result;

    public OpencgaEvent() {
    }

    public OpencgaEvent(String eventId, ObjectMap inputParams, String organizationId, String userId, String token) {
        this.eventId = eventId;
        this.inputParams = inputParams;
        this.organizationId = organizationId;
        this.userId = userId;
        this.token = token;
    }

    public OpencgaEvent(String eventId, ObjectMap inputParams, String organizationId, String resourceId, String resourceUuid,
                        String studyFqn, String studyUuid, String userId, String token, OpenCGAResult<?> result) {
        this.eventId = eventId;
        this.inputParams = inputParams;
        this.organizationId = organizationId;
        this.resourceId = resourceId;
        this.resourceUuid = resourceUuid;
        this.studyFqn = studyFqn;
        this.studyUuid = studyUuid;
        this.userId = userId;
        this.token = token;
        this.result = result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpencgaEvent{");
        sb.append("eventId='").append(eventId).append('\'');
        sb.append(", inputParams=").append(inputParams);
        sb.append(", organizationId='").append(organizationId).append('\'');
        sb.append(", resourceId='").append(resourceId).append('\'');
        sb.append(", resourceUuid='").append(resourceUuid).append('\'');
        sb.append(", studyFqn='").append(studyFqn).append('\'');
        sb.append(", studyUuid='").append(studyUuid).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", result=").append(result);
        sb.append('}');
        return sb.toString();
    }

    public String getEventId() {
        return eventId;
    }

    public OpencgaEvent setEventId(String eventId) {
        this.eventId = eventId;
        return this;
    }

    public ObjectMap getInputParams() {
        return inputParams;
    }

    public OpencgaEvent setInputParams(ObjectMap inputParams) {
        this.inputParams = inputParams;
        return this;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public OpencgaEvent setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
        return this;
    }

    public String getResourceId() {
        return resourceId;
    }

    public OpencgaEvent setResourceId(String resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public String getResourceUuid() {
        return resourceUuid;
    }

    public OpencgaEvent setResourceUuid(String resourceUuid) {
        this.resourceUuid = resourceUuid;
        return this;
    }

    public String getStudyFqn() {
        return studyFqn;
    }

    public OpencgaEvent setStudyFqn(String studyFqn) {
        this.studyFqn = studyFqn;
        return this;
    }

    public String getStudyUuid() {
        return studyUuid;
    }

    public OpencgaEvent setStudyUuid(String studyUuid) {
        this.studyUuid = studyUuid;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public OpencgaEvent setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getToken() {
        return token;
    }

    public OpencgaEvent setToken(String token) {
        this.token = token;
        return this;
    }

    public OpenCGAResult<?> getResult() {
        return result;
    }

    public OpencgaEvent setResult(OpenCGAResult<?> result) {
        this.result = result;
        return this;
    }
}
