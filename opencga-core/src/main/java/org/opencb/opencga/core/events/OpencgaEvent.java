package org.opencb.opencga.core.events;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.EntryParam;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Collections;
import java.util.List;

public class OpencgaEvent {

    private String operationId;
    private String eventId;
    private ObjectMap inputParams;

    private String organizationId;
    private String studyFqn;
    private String studyUuid;

    private List<EntryParam> entries;
    // List<EntryParam> failedEntries; ?? SampleManager - getAcls

    private String userId;
    private String token;

    private OpenCGAResult<?> result;

    private OpencgaEvent() {
    }

    private OpencgaEvent(String operationId, String eventId, ObjectMap inputParams, String organizationId, String userId, String token) {
        this(operationId, eventId, inputParams, organizationId, "", "", Collections.emptyList(), userId, token, null);
    }

    private OpencgaEvent(String operationId, String eventId, ObjectMap inputParams, String organizationId,  String studyFqn,
                         String studyUuid, String userId, String token) {
        this(operationId, eventId, inputParams, organizationId, studyFqn, studyUuid, Collections.emptyList(), userId, token, null);
    }

    private OpencgaEvent(String operationId, String eventId, ObjectMap inputParams, String organizationId, String studyFqn,
                         String studyUuid, List<EntryParam> entries, String userId, String token, OpenCGAResult<?> result) {
        this.operationId = operationId;
        this.eventId = eventId;
        this.inputParams = inputParams;
        this.organizationId = organizationId;
        this.studyFqn = studyFqn;
        this.studyUuid = studyUuid;
        this.entries = entries;
        this.userId = userId;
        this.token = token;
        this.result = result;
    }

    public static OpencgaEvent build(String operationId, String eventId, ObjectMap inputParams, String organizationId, String userId,
                                     String token) {
        return new OpencgaEvent(operationId, eventId, inputParams, organizationId, userId, token);
    }

    public static OpencgaEvent build(String operationId, String eventId, ObjectMap inputParams, String organizationId, String studyFqn,
                                     String studyUuid, String userId, String token) {
        return new OpencgaEvent(operationId, eventId, inputParams, organizationId, studyFqn, studyUuid, userId, token);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpencgaEvent{");
        sb.append("operationId='").append(operationId).append('\'');
        sb.append(", eventId='").append(eventId).append('\'');
        sb.append(", inputParams=").append(inputParams);
        sb.append(", organizationId='").append(organizationId).append('\'');
        sb.append(", studyFqn='").append(studyFqn).append('\'');
        sb.append(", studyUuid='").append(studyUuid).append('\'');
        sb.append(", entries=").append(entries);
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", result=").append(result);
        sb.append('}');
        return sb.toString();
    }

    public String getOperationId() {
        return operationId;
    }

    public OpencgaEvent setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
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

    public List<EntryParam> getEntries() {
        return entries;
    }

    public OpencgaEvent setEntries(List<EntryParam> entries) {
        this.entries = entries;
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
