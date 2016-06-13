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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.AbstractCatalogDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.StudyAcl;

import javax.annotation.Nullable;
import java.util.List;
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


    default Long studyExists(long studyId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), studyId)).first();
    }

    default void checkStudyId(long studyId) throws CatalogDBException {
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

    QueryResult<Study> createStudy(long projectId, Study study, QueryOptions options) throws CatalogDBException;

    //@Deprecated
    //QueryResult<Study> getAllStudies(QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getAllStudiesInProject(long projectId, QueryOptions options) throws CatalogDBException;

    QueryResult<Study> getStudy(long studyId, QueryOptions options) throws CatalogDBException;

    void updateStudyLastActivity(long studyId) throws CatalogDBException;

//    @Deprecated
//    QueryResult<Study> modifyStudy(long studyId, ObjectMap params) throws CatalogDBException;

//  QueryResult modifyStudy(long studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats)
// throws CatalogManagerException;

//    @Deprecated
//    default QueryResult<Study> deleteStudy(long studyId) throws CatalogDBException {
//        return delete(studyId, false);
//    }

    long getStudyId(long projectId, String studyAlias) throws CatalogDBException;

    long getProjectIdByStudyId(long studyId) throws CatalogDBException;

    String getStudyOwnerId(long studyId) throws CatalogDBException;

    /**
     * Obtains the studyAcl given the following parameters. If only the studyId is given, a list containing all the studyAcls will be
     * returned.
     *
     * @param studyId Study id.
     * @param roleId Role id to look for studyAcls.
     * @param members List of members to look for permissions. Can only be existing users or groups.
     * @return A queryResult object containing a list of studyAcls that satisfies the query.
     * @throws CatalogDBException when the studyId does not exist, or the roleId or the members introduced do not exist in the database.
     */
    QueryResult<StudyAcl> getStudyAcl(long studyId, @Nullable String roleId, List<String> members) throws CatalogDBException;

    @Deprecated
    QueryResult<Group> getGroup(long studyId, String userId, String groupId, QueryOptions options) throws CatalogDBException;

    /**
     * Obtains the groups that satisfies the query.
     *
     * @param studyId study id.
     * @param groupId group id.
     * @param userIds List of user ids.
     * @return the list of groups satisfying the query.
     * @throws CatalogDBException when any of the studyId, groupId (if any) or userIds do not exist.
     */
    QueryResult<Group> getGroup(long studyId, @Nullable String groupId, List<String> userIds) throws CatalogDBException;

    /**
     * Adds the list of members to the groupId. If the groupId did not already existed, it creates it.
     *
     * @param studyId studyId
     * @param groupId Group id.
     * @param members List of members that will be added to the group.
     * @return The group that has been updated/created.
     * @throws CatalogDBException when any of the studyId or the members do not exist.
     */
    QueryResult<Group> addMembersToGroup(long studyId, String groupId, List<String> members) throws CatalogDBException;

    /**
     * Removes the list of members from the group.
     *
     * @param studyId study id.
     * @param groupId Group id where the user members belong to.
     * @param members List of members that are going to be removed from the group.
     * @throws CatalogDBException when any of the studyId, groupId or members do not exist.
     */
    void removeMembersFromGroup(long studyId, String groupId, List<String> members) throws CatalogDBException;

    /**
     * Adds the permissions defined in the roleId to the list of members.
     *
     * @param studyId study id.
     * @param roleId Role that will be applied to the members.
     * @param members List of members (users and/or groups).
     * @param override Boolean parameter indicating whether to set the Acl when the members already had other Acl set. In that case, the old
     *                 Acl will be removed and the new one will be set. Otherwise, an exception will be raised.
     * @return A queryResult with the studyAcl after the update.
     * @throws CatalogDBException when any of the studyId, roleId or members do not exist or if there is a user inside a group defined in
     * members that already have a permission created.
     */
    QueryResult<StudyAcl> setStudyAcl(long studyId, String roleId, List<String> members, boolean override) throws CatalogDBException;

    /**
     * Remove the permissions for the members.
     *
     * @param studyId study id.
     * @param members List of members whose permissions will be removed.
     * @throws CatalogDBException when any of the studyId or the members do not exist.
     */
    void unsetStudyAcl(long studyId, List<String> members) throws CatalogDBException;

//    QueryResult<StudyAcl> getStudyAcls(long studyId, String userId, String groupId) throws CatalogDBException;

    @Deprecated
    QueryResult<Role> getRole(long studyId, String userId, String groupId, String roleId, QueryOptions options) throws CatalogDBException;

    /*
     * VariableSet Methods
     * ***************************
     */

    /**
     * The method will obtain the number of variableSets with the same id.
     * @param variableSetId id of the variableSet.
     * @return the count of variableSets with the variableSetId.
     */
    Long variableSetExists(long variableSetId);

    default Long variableSetExists(String variableSetName, long studyId) throws CatalogDBException {
        Query query = new Query(QueryParams.VARIABLE_SET_NAME.key(), variableSetName).append(QueryParams.ID.key(), studyId);
        return count(query).first();
    }

    default void checkVariableSetExists(long variableSetId) throws CatalogDBException {
        if (variableSetId < 0) {
            throw CatalogDBException.newInstance("VariableSet id '{}' is not valid: ", variableSetId);
        }
        Long count = variableSetExists(variableSetId);
        if (count <= 0) {
            throw CatalogDBException.newInstance("VariableSet id '{}' does not exist", variableSetId);
        } else if (count > 1) {
            throw CatalogDBException.newInstance("'{}' documents found with the VariableSet id '{}'", count, variableSetId);
        }
    }

    default void checkVariableSetExists(String variableSetName, long studyId) throws CatalogDBException {
        Long count = variableSetExists(variableSetName, studyId);
        if (count <= 0) {
            throw CatalogDBException.newInstance("VariableSet name '{}' does not exist", variableSetName);
        } else if (count > 1) {
            throw CatalogDBException.newInstance("'{}' documents found with the VariableSet name '{}' in study '{}'", count,
                    variableSetName, studyId);
        }
    }

    QueryResult<VariableSet> createVariableSet(long studyId, VariableSet variableSet) throws CatalogDBException;

    QueryResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable) throws CatalogDBException;

    QueryResult<VariableSet> renameFieldVariableSet(long variableSetId, String oldName, String newName) throws CatalogDBException;

    QueryResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String name) throws CatalogDBException;

    QueryResult<VariableSet> getVariableSet(long variableSetId, QueryOptions options) throws CatalogDBException;

    QueryResult<VariableSet> getAllVariableSets(long studyId, QueryOptions queryOptions) throws CatalogDBException;

    QueryResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions) throws CatalogDBException;

    long getStudyIdByVariableSetId(long variableSetId) throws CatalogDBException;

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        ALIAS("alias", TEXT_ARRAY, ""),
        CREATION_DATE("creationDate", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        CIPHER("cipher", TEXT, ""),
        STATUS_STATUS("status.status", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        LAST_ACTIVITY("lastActivity", TEXT_ARRAY, ""),
        DISK_USAGE("diskUsage", INTEGER_ARRAY, ""),
        URI("uri", TEXT_ARRAY, ""),
        ACLS("acls", TEXT_ARRAY, ""),
        ACLS_ROLE("acls.role", TEXT, ""),
        ACLS_USERS("acls.users", TEXT_ARRAY, ""),
        ACLS_PERMISSIONS("acls.permissions", TEXT_ARRAY, ""),
        PROJECT_ID("projectId", INTEGER_ARRAY, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]",
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATS("stats", TEXT, ""),

        GROUPS("groups", TEXT_ARRAY, ""),
        GROUP_ID("groups.id", TEXT_ARRAY, ""),
        GROUP_USER_IDS("groups.userIds", TEXT_ARRAY, ""),

        ROLES("roles", TEXT_ARRAY, ""),
        ROLES_ID("roles.id", TEXT, ""),
        ROLES_USERS("roles.users", TEXT_ARRAY, ""),
        ROLES_PERMISSIONS("roles.permissions", TEXT, ""),

        EXPERIMENT_ID("experiments.id", INTEGER_ARRAY, ""),
        EXPERIMENT_NAME("experiments.name", TEXT_ARRAY, ""),
        EXPERIMENT_TYPE("experiment.type", TEXT_ARRAY, ""),
        EXPERIMENT_PLATFORM("experiments.platform", TEXT_ARRAY, ""),
        EXPERIMENT_MANUFACTURER("experiments.manufacturer", TEXT_ARRAY, ""),
        EXPERIMENT_DATE("experiments.date", TEXT_ARRAY, ""),
        EXPERIMENT_LAB("experiments.lab", TEXT_ARRAY, ""),
        EXPERIMENT_CENTER("experiments.center", TEXT_ARRAY, ""),
        EXPERIMENT_RESPONSIBLE("experiments.responsible", TEXT_ARRAY, ""),

//        FILE_ID("files.id", INTEGER_ARRAY, ""),
//        FILE_NAME("files.name", TEXT_ARRAY, ""),
//        FILE_TYPE("files.type", TEXT_ARRAY, ""),
//        FILE_FORMAT("files.format", TEXT_ARRAY, ""),
//        FILE_BIOFORMAT("files.bioformat", TEXT_ARRAY, ""),
//        FILE_DISK_USAGE("files.diskUsage", INTEGER_ARRAY, ""),

//        JOB_ID("jobs.id", INTEGER_ARRAY, ""),
//        JOB_NAME("jobs.name", TEXT_ARRAY, ""),
//        JOB_USER_ID("jobs.userId", TEXT_ARRAY, ""),
//        JOB_TOOL_NAME("jobs.toolName", TEXT_ARRAY, ""),
//        JOB_DATE("jobs.date", TEXT_ARRAY, ""),
//        JOB_STATUS("jobs.status", TEXT_ARRAY, ""),
//        JOB_DISK_USAGE("jobs.diskUsage", DECIMAL, ""),

//        INDIVIDUAL_ID("individuals.id", INTEGER_ARRAY, ""),
//        INDIVIDUAL_NAME("individuals.name", TEXT_ARRAY, ""),
//        INDIVIDUAL_FATHER_ID("individuals.fatherId", INTEGER_ARRAY, ""),
//        INDIVIDUAL_MOTHER_ID("individuals.motherId", INTEGER_ARRAY, ""),
//        INDIVIDUAL_FAMILY("individuals.family", TEXT_ARRAY, ""),
//        INDIVIDUAL_RACE("individuals.race", TEXT_ARRAY, ""),

//        SAMPLE_ID("samples.id", INTEGER_ARRAY, ""),
//        SAMPLE_NAME("samples.name", TEXT_ARRAY, ""),
//        SAMPLE_SOURCE("samples.source", TEXT_ARRAY, ""),
//        SAMPLE_INDIVIDUAL_ID("samples.individualId", INTEGER_ARRAY, ""),

//        DATASET_ID("datasets.id", INTEGER_ARRAY, ""),
//        DATASET_NAME("datasets.name", TEXT_ARRAY, ""),

        COHORTS("cohorts", TEXT_ARRAY, ""),
//        COHORT_ID("cohorts.id", INTEGER_ARRAY, ""),
//        COHORT_NAME("cohorts.name", TEXT_ARRAY, ""),
//        COHORT_TYPE("cohorts.type", TEXT_ARRAY, ""),

        VARIABLE_SET("variableSets", TEXT_ARRAY, ""),
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

    enum VariableSetParams implements QueryParam {
        ID("id", DOUBLE, ""),
        NAME("name", TEXT, ""),
        UNIQUE("unique", BOOLEAN, ""),
        DESCRIPTION("description", TEXT, ""),
        VARIABLE("variables", TEXT_ARRAY, ""),
        VARIABLE_ID("variables.id", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        NATTRIBUTES("nattributes", DECIMAL, "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"),
        BATTRIBUTES("battributes", BOOLEAN, "Format: <key><operation><true|false> where <operation> is [==|!=]"),
        STUDY_ID("studyId", DECIMAL, "");

        private static Map<String, VariableSetParams> map;
        static {
            map = new LinkedMap();
            for (VariableSetParams params : VariableSetParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        VariableSetParams(String key, Type type, String description) {
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

        public static Map<String, VariableSetParams> getMap() {
            return map;
        }

        public static VariableSetParams getParam(String key) {
            return map.get(key);
        }
    }

    @Deprecated
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
