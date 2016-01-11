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

package org.opencb.opencga.catalog.db.api2;

import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.db.AbstractCatalogDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.VariableSet;

import static org.opencb.commons.datastore.core.QueryParam.Type.INTEGER_ARRAY;
import static org.opencb.commons.datastore.core.QueryParam.Type.DECIMAL;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogStudyDBAdaptor extends CatalogDBAdaptor<Study> {

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        ALIAS("alias", TEXT_ARRAY, ""),
        CREATOR_ID("creatorId", TEXT_ARRAY, ""),
        STATUS("status", TEXT_ARRAY, ""),
        LAST_ACTIVITY("lastActivity", TEXT_ARRAY, ""),
        PROJECT_ID("_projectId", TEXT_ARRAY, ""),

        GROUP_ID("group.id", INTEGER_ARRAY, ""),

        EXPERIMENT_ID("experiment.id", INTEGER_ARRAY, ""),
        EXPERIMENT_NAME("experiment.name", TEXT_ARRAY, ""),
        EXPERIMENT_TYPE("experiment.type", TEXT_ARRAY, ""),
        EXPERIMENT_PLATFORM("experiment.platform", TEXT_ARRAY, ""),
        EXPERIMENT_MANUFACTURER("experiment.manufacturer", TEXT_ARRAY, ""),
        EXPERIMENT_DATE("experiment.date", TEXT_ARRAY, ""),
        EXPERIMENT_LAB("experiment.lab", TEXT_ARRAY, ""),
        EXPERIMENT_CENTER("experiment.center", TEXT_ARRAY, ""),
        EXPERIMENT_RESPONSIBLE("experiment.responsible", TEXT_ARRAY, ""),

        FILE_ID("file.id", INTEGER_ARRAY, ""),
        FILE_NAME("file.name", TEXT_ARRAY, ""),
        FILE_TYPE("file.type", TEXT_ARRAY, ""),
        FILE_FORMAT("file.format", TEXT_ARRAY, ""),
        FILE_BIOFORMAT("file.bioformat", TEXT_ARRAY, ""),

        JOB_ID("job.id", INTEGER_ARRAY, ""),
        JOB_NAME("job.name", TEXT_ARRAY, ""),
        JOB_USER_ID("job.userId", TEXT_ARRAY, ""),
        JOB_TOOL_NAME("job.toolName", TEXT_ARRAY, ""),
        JOB_DATE("job.date", TEXT_ARRAY, ""),
        JOB_STATUS("job.status", TEXT_ARRAY, ""),
        JOB_DISK_USAGE("job.diskUsage", DECIMAL, ""),

        INDIVIDUAL_ID("individual.id", INTEGER_ARRAY, ""),
        INDIVIDUAL_NAME("individual.name", TEXT_ARRAY, ""),
        INDIVIDUAL_FATHER_ID("individual.fatherId", INTEGER_ARRAY, ""),
        INDIVIDUAL_MOTHER_ID("individual.motherId", INTEGER_ARRAY, ""),
        INDIVIDUAL_FAMILY("individual.family", TEXT_ARRAY, ""),
        INDIVIDUAL_RACE("individual.race", TEXT_ARRAY, ""),

        SAMPLE_ID("sample.id", INTEGER_ARRAY, ""),
        SAMPLE_NAME("sample.name", TEXT_ARRAY, ""),
        SAMPLE_SOURCE("sample.source", TEXT_ARRAY, ""),
        SAMPLE_INDIVIDUAL_ID("sample.individualId", INTEGER_ARRAY, ""),

        DATASET_ID("dataset.id", INTEGER_ARRAY, ""),
        DATASET_NAME("dataset.name", TEXT_ARRAY, ""),

        COHORT_ID("cohort.id", INTEGER_ARRAY, ""),
        COHORT_NAME("cohort.name", TEXT_ARRAY, ""),
        COHORT_TYPE("cohort.type", TEXT_ARRAY, "");

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
    }

    /**
     * Study methods
     * ***************************
     */


    default boolean studyExists(int studyId) {
        return count(new Query(QueryParams.ID.key(), studyId)).first() > 0;
    }

    default void checkStudyId(int studyId) throws CatalogDBException {
        if (studyId < 0) {
            throw CatalogDBException.newInstance("Study id '{}' is not valid: ", studyId);
        }

        if (!studyExists(studyId)) {
            throw CatalogDBException.newInstance("Study id '{}' does not exist", studyId);
        }
    }

    QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getAllStudies(QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getAllStudiesInProject(int projectId, QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException;

    void updateStudyLastActivity(int studyId) throws CatalogDBException;

//  QueryResult modifyStudy(int studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats)
// throws CatalogManagerException;

    QueryResult<Study> modifyStudy(int studyId, ObjectMap params) throws CatalogDBException;

    default QueryResult<Study> deleteStudy(int studyId) throws CatalogDBException {
        Query query = new Query(CatalogStudyDBAdaptor.QueryParams.ID.key(), studyId);
        QueryResult<Study> sampleQueryResult = get(query, new QueryOptions());
        if (sampleQueryResult.getResult().size() == 1) {
            QueryResult<Long> delete = delete(query);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Study id '{}' has not been deleted", studyId);
            }
        } else {
            throw CatalogDBException.newInstance("Study id '{}' does not exist", studyId);
        }
        return sampleQueryResult;
    }

    int getStudyId(int projectId, String studyAlias) throws CatalogDBException;

    int getProjectIdByStudyId(int studyId) throws CatalogDBException;

    String getStudyOwnerId(int studyId) throws CatalogDBException;

    QueryResult<Group> getGroup(int studyId, String userId, String groupId, QueryOptions options) throws CatalogDBException;

    QueryResult<Group> addMemberToGroup(int studyId, String groupId, String userId) throws CatalogDBException;

    QueryResult<Group> removeMemberFromGroup(int studyId, String groupId, String userId) throws CatalogDBException;

    /**
     * VariableSet Methods
     * ***************************
     */

    QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException;

    QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException;

    QueryResult<VariableSet> getAllVariableSets(int studyId, QueryOptions queryOptions) throws CatalogDBException;

    QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions) throws CatalogDBException;

    int getStudyIdByVariableSetId(int variableSetId) throws CatalogDBException;

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
        battributes("attributes", Type.BOOLEAN, ""),;

        final private String _key;
        final private String _description;
        final private Type _type;
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
