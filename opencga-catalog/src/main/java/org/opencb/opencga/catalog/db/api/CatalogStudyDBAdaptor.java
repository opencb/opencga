/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections.map.LinkedMap;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.db.AbstractCatalogDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.VariableSet;

import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogStudyDBAdaptor extends CatalogDBAdaptor<Study> {

    /*
     * Study methods
     * ***************************
     */


    default Long studyExists(int studyId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), studyId)).first();
    }

    default void checkStudyId(int studyId) throws CatalogDBException {
        if (studyId < 0) {
            throw CatalogDBException.newInstance("Study id '{}' is not valid: ", studyId);
        }
        Long count = studyExists(studyId);
        if (count <= 0) {
            throw CatalogDBException.newInstance("Study id '{}' does not exist", studyId);
        } else if (count > 1) {
            throw CatalogDBException.newInstance("'{}' documents found with the Study id '{}'", count, studyId);
        }
    }

    QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException;

    @Deprecated
    QueryResult<Study> getAllStudies(QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getAllStudiesInProject(int projectId, QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException;

    void updateStudyLastActivity(int studyId) throws CatalogDBException;

    @Deprecated
    QueryResult<Study> modifyStudy(int studyId, ObjectMap params) throws CatalogDBException;

//  QueryResult modifyStudy(int studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats)
// throws CatalogManagerException;

    @Deprecated
    default QueryResult<Study> deleteStudy(int studyId) throws CatalogDBException {
        return delete(studyId);
    }

    int getStudyId(int projectId, String studyAlias) throws CatalogDBException;

    int getProjectIdByStudyId(int studyId) throws CatalogDBException;

    String getStudyOwnerId(int studyId) throws CatalogDBException;

    QueryResult<Group> getGroup(int studyId, String userId, String groupId, QueryOptions options) throws CatalogDBException;

    QueryResult<Group> addMemberToGroup(int studyId, String groupId, String userId) throws CatalogDBException;

    QueryResult<Group> removeMemberFromGroup(int studyId, String groupId, String userId) throws CatalogDBException;

    /*
     * VariableSet Methods
     * ***************************
     */

    QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException;

    QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException;

    QueryResult<VariableSet> getAllVariableSets(int studyId, QueryOptions queryOptions) throws CatalogDBException;

    QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions) throws CatalogDBException;

    int getStudyIdByVariableSetId(int variableSetId) throws CatalogDBException;

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        ALIAS("alias", TEXT_ARRAY, ""),
        CREATOR_ID("creatorId", TEXT_ARRAY, ""),
        STATUS("status", TEXT_ARRAY, ""),
        LAST_ACTIVITY("lastActivity", TEXT_ARRAY, ""),
        DISK_USAGE("diskUsage", INTEGER_ARRAY, ""),
        PROJECT_ID("projectId", INTEGER_ARRAY, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"

        GROUP_ID("groups.id", TEXT_ARRAY, ""),
        GROUP_USER_IDS("groups.userIds", TEXT_ARRAY, ""),

        EXPERIMENT_ID("experiments.id", INTEGER_ARRAY, ""),
        EXPERIMENT_NAME("experiments.name", TEXT_ARRAY, ""),
        EXPERIMENT_TYPE("experiment.type", TEXT_ARRAY, ""),
        EXPERIMENT_PLATFORM("experiments.platform", TEXT_ARRAY, ""),
        EXPERIMENT_MANUFACTURER("experiments.manufacturer", TEXT_ARRAY, ""),
        EXPERIMENT_DATE("experiments.date", TEXT_ARRAY, ""),
        EXPERIMENT_LAB("experiments.lab", TEXT_ARRAY, ""),
        EXPERIMENT_CENTER("experiments.center", TEXT_ARRAY, ""),
        EXPERIMENT_RESPONSIBLE("experiments.responsible", TEXT_ARRAY, ""),

        FILE_ID("files.id", INTEGER_ARRAY, ""),
        FILE_NAME("files.name", TEXT_ARRAY, ""),
        FILE_TYPE("files.type", TEXT_ARRAY, ""),
        FILE_FORMAT("files.format", TEXT_ARRAY, ""),
        FILE_BIOFORMAT("files.bioformat", TEXT_ARRAY, ""),
        FILE_DISK_USAGE("files.diskUsage", INTEGER_ARRAY, ""),

        JOB_ID("jobs.id", INTEGER_ARRAY, ""),
        JOB_NAME("jobs.name", TEXT_ARRAY, ""),
        JOB_USER_ID("jobs.userId", TEXT_ARRAY, ""),
        JOB_TOOL_NAME("jobs.toolName", TEXT_ARRAY, ""),
        JOB_DATE("jobs.date", TEXT_ARRAY, ""),
        JOB_STATUS("jobs.status", TEXT_ARRAY, ""),
        JOB_DISK_USAGE("jobs.diskUsage", DECIMAL, ""),

        INDIVIDUAL_ID("individuals.id", INTEGER_ARRAY, ""),
        INDIVIDUAL_NAME("individuals.name", TEXT_ARRAY, ""),
        INDIVIDUAL_FATHER_ID("individuals.fatherId", INTEGER_ARRAY, ""),
        INDIVIDUAL_MOTHER_ID("individuals.motherId", INTEGER_ARRAY, ""),
        INDIVIDUAL_FAMILY("individuals.family", TEXT_ARRAY, ""),
        INDIVIDUAL_RACE("individuals.race", TEXT_ARRAY, ""),

        SAMPLE_ID("samples.id", INTEGER_ARRAY, ""),
        SAMPLE_NAME("samples.name", TEXT_ARRAY, ""),
        SAMPLE_SOURCE("samples.source", TEXT_ARRAY, ""),
        SAMPLE_INDIVIDUAL_ID("samples.individualId", INTEGER_ARRAY, ""),

        DATASET_ID("datasets.id", INTEGER_ARRAY, ""),
        DATASET_NAME("datasets.name", TEXT_ARRAY, ""),

        COHORT_ID("cohorts.id", INTEGER_ARRAY, ""),
        COHORT_NAME("cohorts.name", TEXT_ARRAY, ""),
        COHORT_TYPE("cohorts.type", TEXT_ARRAY, ""),

        VARIABLE_SET_ID("variableSets.id", INTEGER_ARRAY, ""),
        VARIABLE_SET_NAME("variableSets.name", TEXT_ARRAY, ""),
        VARIABLE_SET_DESCRIPTION("variableSets.description", TEXT_ARRAY, "");

        private static Map<String, QueryParams> map;
        static {
            map = new LinkedMap();
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    enum StudyFilterOptions implements AbstractCatalogDBAdaptor.FilterOption {
        id(Type.NUMERICAL, ""),
        projectId(Type.NUMERICAL, ""),
        name(Type.TEXT, ""),
        alias(Type.TEXT, ""),
        type(Type.TEXT, ""),
        groups(Type.TEXT, ""),
        creatorId(Type.TEXT, ""),
        creationDate(Type.TEXT, ""),
        status(Type.TEXT, ""),
        lastActivity(Type.TEXT, ""),
        stats(Type.TEXT, ""),
        attributes(Type.TEXT, ""),
        nattributes("attributes", Type.NUMERICAL, ""),
        battributes("attributes", Type.BOOLEAN, "");

        private final String _key;
        private final String _description;
        private final Type _type;

        StudyFilterOptions(String key, Type type, String description) {
            this._key = key;
            this._description = description;
            this._type = type;
        }

        StudyFilterOptions(Type type, String description) {
            this._key = name();
            this._description = description;
            this._type = type;
        }

        @Override
        public String getKey() {
            return _key;
        }

        @Override
        public String getDescription() {
            return _description;
        }

        @Override
        public Type getType() {
            return _type;
        }
    }
}
