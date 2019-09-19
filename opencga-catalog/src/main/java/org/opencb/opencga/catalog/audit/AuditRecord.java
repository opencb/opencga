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
import org.opencb.commons.datastore.core.Query;

import java.util.Date;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditRecord {

    private String userId;
    private String apiVersion;
    private String operationUuid;
    private String id;
    private String uuid;
    private String studyId;
    private String studyUuid;

    private Query query;
    private ObjectMap params;

    private Entity entity;
    private Action action;

    private int status;

    private Date date;
    private ObjectMap attributes;

    public static final int SUCCESS = 0;
    public static final int ERROR = 1;

    public AuditRecord() {
    }

    public AuditRecord(String userId, String apiVersion, String operationUuid, String id, String uuid, String studyId, String studyUuid,
                       Query query, ObjectMap params, Entity entity, Action action, int status, Date date, ObjectMap attributes) {
        this.userId = userId;
        this.apiVersion = apiVersion;
        this.operationUuid = operationUuid;
        this.id = id;
        this.uuid = uuid;
        this.studyId = studyId;
        this.studyUuid = studyUuid;
        this.query = query;
        this.params = params;
        this.entity = entity;
        this.action = action;
        this.status = status;
        this.date = date;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AuditRecord{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", apiVersion='").append(apiVersion).append('\'');
        sb.append(", operationUuid='").append(operationUuid).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", studyUuid='").append(studyUuid).append('\'');
        sb.append(", query=").append(query);
        sb.append(", params=").append(params);
        sb.append(", entity=").append(entity);
        sb.append(", action=").append(action);
        sb.append(", status=").append(status);
        sb.append(", date=").append(date);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
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

    public String getOperationUuid() {
        return operationUuid;
    }

    public AuditRecord setOperationUuid(String operationUuid) {
        this.operationUuid = operationUuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public AuditRecord setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public AuditRecord setUuid(String uuid) {
        this.uuid = uuid;
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

    public Query getQuery() {
        return query;
    }

    public AuditRecord setQuery(Query query) {
        this.query = query;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public AuditRecord setParams(ObjectMap params) {
        this.params = params;
        return this;
    }

    public Entity getEntity() {
        return entity;
    }

    public AuditRecord setEntity(Entity entity) {
        this.entity = entity;
        return this;
    }

    public Action getAction() {
        return action;
    }

    public AuditRecord setAction(Action action) {
        this.action = action;
        return this;
    }

    public int getStatus() {
        return status;
    }

    public AuditRecord setStatus(int status) {
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

        IMPORT_GLOBAL_PANEL,

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
