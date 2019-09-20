/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.catalog.audit;


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.result.Error;

import java.util.Date;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditRecord {

    /**
     * AuditRecord timestamp UUID
     */
    private String id;

    /**
     * operationID timestamp UUID
     */
    private String operationId;

    private String userId;
    private String apiVersion;

    private Action action;

    private Entity entity;
    private String resourceId;
    private String resourceUuid;

    private String studyId;
    private String studyUuid;

    private ObjectMap params;

    private Status status;

    private Date date;
    private ObjectMap attributes;

    public AuditRecord() {
    }

    public AuditRecord(String id, String operationId, String userId, String apiVersion, Action action, Entity entity, String resourceId,
                       String resourceUuid, String studyId, String studyUuid, ObjectMap params, Status status, Date date,
                       ObjectMap attributes) {
        this.id = id;
        this.operationId = operationId;
        this.userId = userId;
        this.apiVersion = apiVersion;
        this.action = action;
        this.entity = entity;
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
        sb.append(", entity=").append(entity);
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

    public Action getAction() {
        return action;
    }

    public AuditRecord setAction(Action action) {
        this.action = action;
        return this;
    }

    public Entity getEntity() {
        return entity;
    }

    public AuditRecord setEntity(Entity entity) {
        this.entity = entity;
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

// Resource
    public enum Entity {
        USER,
        PROJECT,
        STUDY,
        FILE,
        SAMPLE,
        JOB,
        INDIVIDUAL,
        COHORT,
        PANEL,
        FAMILY,
        CLINICAL,
        INTERPRETATION,
        VARIANT,
        ALIGNMENT
    }

    public enum Action {
        CREATE,
        UPDATE,
        INFO,
        SEARCH,
        COUNT,
        DELETE,
        DOWNLOAD,
        INDEX,
        CHANGE_PERMISSION,

        LOGIN,
        CHANGE_USER_PASSWORD,
        RESET_USER_PASSWORD,
        CHANGE_USER_CONFIG,
        FETCH_USER_CONFIG,

        INCREMENT_PROJECT_RELEASE,

        FETCH_STUDY_GROUPS,
        ADD_STUDY_GROUP,
        REMOVE_STUDY_GROUP,
        UPDATE_USERS_FROM_STUDY_GROUP,
        FETCH_STUDY_PERMISSION_RULES,
        ADD_STUDY_PERMISSION_RULE,
        REMOVE_STUDY_PERMISSION_RULE,
        FETCH_ACLS,
        UPDATE_ACLS,
        FETCH_VARIABLE_SET,
        ADD_VARIABLE_SET,
        DELETE_VARIABLE_SET,
        ADD_VARIABLE_TO_VARIABLE_SET,
        REMOVE_VARIABLE_FROM_VARIABLE_SET,

        AGGREGATION_STATS,

        UPLOAD,
        LINK,
        UNLINK,
        GREP,
        TREE,

        VISIT,

//        IMPORT_GLOBAL_PANEL,

        IMPORT_EXTERNAL_USERS,
        IMPORT_EXTERNAL_GROUP_OF_USERS,
        SYNC_EXTERNAL_GROUP_OF_USERS,

        // Variants
        SAMPLE_DATA,
        FACET
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
