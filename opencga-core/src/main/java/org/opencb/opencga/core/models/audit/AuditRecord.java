/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.models.audit;


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.opencga.core.models.common.Enums;

import java.util.Date;

public class AuditRecord {

    /**
     * AuditRecord timestamp UUID.
     */
    private String id;

    /**
     * operationID timestamp UUID.
     */
    private String operationId;

    /**
     * User performing the action.
     */
    private String userId;
    /**
     * OpenCGA API version.
     */
    private String apiVersion;

    /**
     * Action performed (CREATE, SEARCH, DOWNLOAD...).
     */
    private Enums.Action action;

    /**
     * Involved resource (User, Study, Sample, File...).
     */
    private Enums.Resource resource;
    /**
     * Id of the involved resource.
     */
    private String resourceId;
    /**
     * UUID of the involved resource.
     */
    private String resourceUuid;

    /**
     * Study id corresponding to the involved resource. Does not apply to User or Project resources.
     */
    private String studyId;
    /**
     * Study UUID corresponding to the involved resource. Does not apply to User or Project resources.
     */
    private String studyUuid;

    /**
     * User params sent by the user. All the parameters considered and sent by the user to perform the action.
     */
    private ObjectMap params;

    /**
     * Final result of the action: success or error. In case of error, it will also contain the error code and the error message.
     */
    private Status status;

    /**
     * Date of the audit record.
     */
    private Date date;
    /**
     * Any additional information that might have not been covered by the data model.
     */
    private ObjectMap attributes;

    public AuditRecord() {
    }

    public AuditRecord(String id, String operationId, String userId, String apiVersion, Enums.Action action, Enums.Resource resource,
                       String resourceId, String resourceUuid, String studyId, String studyUuid, ObjectMap params, Status status, Date date,
                       ObjectMap attributes) {
        this.id = id;
        this.operationId = operationId;
        this.userId = userId;
        this.apiVersion = apiVersion;
        this.action = action;
        this.resource = resource;
        this.resourceId = resourceId;
        this.resourceUuid = resourceUuid;
        this.studyId = studyId;
        this.studyUuid = studyUuid;
        this.params = params;
        this.status = status;
        this.date = date;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AuditRecord{");
        sb.append("id='").append(id).append('\'');
        sb.append(", operationId='").append(operationId).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", apiVersion='").append(apiVersion).append('\'');
        sb.append(", action=").append(action);
        sb.append(", resource=").append(resource);
        sb.append(", resourceId='").append(resourceId).append('\'');
        sb.append(", resourceUuid='").append(resourceUuid).append('\'');
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", studyUuid='").append(studyUuid).append('\'');
        sb.append(", params=").append(params);
        sb.append(", status=").append(status);
        sb.append(", date=").append(date);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public AuditRecord setId(String id) {
        this.id = id;
        return this;
    }

    public String getOperationId() {
        return operationId;
    }

    public AuditRecord setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public AuditRecord setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public AuditRecord setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public Enums.Action getAction() {
        return action;
    }

    public AuditRecord setAction(Enums.Action action) {
        this.action = action;
        return this;
    }

    public Enums.Resource getResource() {
        return resource;
    }

    public AuditRecord setResource(Enums.Resource resource) {
        this.resource = resource;
        return this;
    }

    public String getResourceId() {
        return resourceId;
    }

    public AuditRecord setResourceId(String resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public String getResourceUuid() {
        return resourceUuid;
    }

    public AuditRecord setResourceUuid(String resourceUuid) {
        this.resourceUuid = resourceUuid;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public AuditRecord setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getStudyUuid() {
        return studyUuid;
    }

    public AuditRecord setStudyUuid(String studyUuid) {
        this.studyUuid = studyUuid;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public AuditRecord setParams(ObjectMap params) {
        this.params = params;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public AuditRecord setStatus(Status status) {
        this.status = status;
        return this;
    }

    public Date getDate() {
        return date;
    }

    public AuditRecord setDate(Date date) {
        this.date = date;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public AuditRecord setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }

    public static class Status {
        private Result name;
        private Error error;

        public Status() {
        }

        public Status(Result name) {
            this.name = name;
        }

        public Status(Result name, Error error) {
            this.name = name;
            this.error = error;
        }

        public enum Result {
            SUCCESS,
            ERROR
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Status{");
            sb.append("name=").append(name);
            sb.append(", error=").append(error);
            sb.append('}');
            return sb.toString();
        }

        public Result getName() {
            return name;
        }

        public Status setName(Result name) {
            this.name = name;
            return this;
        }

        public Error getError() {
            return error;
        }

        public Status setError(Error error) {
            this.error = error;
            return this;
        }
    }

    public static class Result {

        private int dbTime;
        private int numMatches;
        private int numModified;
        private String warning;
        private String error;

        public Result() {
        }

        public Result(int dbTime, int numMatches, String warning, String error) {
            this.dbTime = dbTime;
            this.numMatches = numMatches;
            this.warning = warning;
            this.error = error;
        }

        public Result(int dbTime, int numMatches, int numModified, String warning, String error) {
            this.dbTime = dbTime;
            this.numMatches = numMatches;
            this.numModified = numModified;
            this.warning = warning;
            this.error = error;
        }

        @Override
        public String toString() {
            return "Result{"
                    + "dbTime=" + dbTime
                    + ", numMatches=" + numMatches
                    + ", numModified=" + numModified
                    + ", warning='" + warning + '\''
                    + ", error='" + error + '\''
                    + '}';
        }

        public int getDbTime() {
            return dbTime;
        }

        public void setDbTime(int dbTime) {
            this.dbTime = dbTime;
        }

        public int getNumMatches() {
            return numMatches;
        }

        public void setNumMatches(int numMatches) {
            this.numMatches = numMatches;
        }

        public int getNumModified() {
            return numModified;
        }

        public void setNumModified(int numModified) {
            this.numModified = numModified;
        }

        public String getWarning() {
            return warning;
        }

        public void setWarning(String warning) {
            this.warning = warning;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }
    }
}
