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
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.VariableSet;

import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface CatalogStudyDBAdaptor extends CatalogDBAdaptor<Study> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        ALIAS("alias", TEXT_ARRAY, ""),
        CREATOR_ID("creatorId", TEXT_ARRAY, ""),
        STATUS("status", TEXT_ARRAY, ""),
        LAST_ACTIVITY("lastActivity", TEXT_ARRAY, ""),

        FILE_ID("file.id", TEXT_ARRAY, ""),

        ;

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
    QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException;

    default boolean studyExists(int studyId) {
        return count(new Query(QueryParams.ID.key(), studyId)).first() == 0;
    }

    void checkStudyId(int studyId) throws CatalogDBException;

    QueryResult<Study> getAllStudies(QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getAllStudiesInProject(int projectId, QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException;

    QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException;

    void updateStudyLastActivity(int studyId) throws CatalogDBException;

//  QueryResult modifyStudy(int studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats)
// throws CatalogManagerException;

    QueryResult<Study> modifyStudy(int studyId, ObjectMap params) throws CatalogDBException;

    QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException;

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
