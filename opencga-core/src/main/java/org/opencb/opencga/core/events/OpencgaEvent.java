package org.opencb.opencga.core.events;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.response.OpenCGAResult;

public class OpencgaEvent {

    private String eventId;
    private ObjectMap inputParams;
    private String organizationId;
    private String userId;
    private String study;
    private String id;
    private String token;
    private OpenCGAResult<?> result;

    public OpencgaEvent() {
    }

    public OpencgaEvent(String organizationId, String eventId, ObjectMap inputParams, String userId, String study, String id, String token,
                        OpenCGAResult<?> result) {
        this.organizationId = organizationId;
        this.eventId = eventId;
        this.inputParams = inputParams;
        this.userId = userId;
        this.study = study;
        this.id = id;
        this.token = token;
        this.result = result;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public OpencgaEvent setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
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

    public String getUserId() {
        return userId;
    }

    public OpencgaEvent setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public OpencgaEvent setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getId() {
        return id;
    }

    public OpencgaEvent setId(String id) {
        this.id = id;
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
