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

package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserFilter;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface UserDBAdaptor extends DBAdaptor<User> {

    /*
     * User methods
     */
    default boolean exists(String userId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.ID.key(), userId)).getNumMatches() > 0;
    }

    default void checkId(String userId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (StringUtils.isEmpty(userId)) {
            throw CatalogDBException.newInstance("User id '{}' is not valid: ", userId);
        }

        if (!exists(userId)) {
            throw CatalogDBException.newInstance("User id '{}' does not exist", userId);
        }
    }

    default void checkIds(List<String> userIds) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (userIds == null || userIds.isEmpty()) {
            throw CatalogDBException.newInstance("No users to be checked.");
        }

        Set<String> userSet = new HashSet<>(userIds);

        Query query = new Query(QueryParams.ID.key(), userSet);
        if (count(query).getNumMatches() < userSet.size()) {
            throw CatalogDBException.newInstance("Some users do not exist.");
        }
    }

    void authenticate(String userId, String password) throws CatalogDBException, CatalogAuthenticationException;

    OpenCGAResult insert(User user, String password, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<User> get(String userId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

//    @Deprecated
//    default OpenCGAResult<User> modifyUser(String userId, ObjectMap parameters) throws CatalogDBException {
//        return update(userId, parameters);
//    }

    OpenCGAResult update(String userId, ObjectMap parameters)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

//    @Deprecated
//    default OpenCGAResult<User> deleteUser(String userId) throws CatalogDBException {
//        return delete(userId, false);
//    }

    OpenCGAResult delete(String userId, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult changePassword(String userId, String oldPassword, String newPassword)
            throws CatalogDBException, CatalogAuthenticationException;

    OpenCGAResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException;

    // Config operations
    OpenCGAResult setConfig(String userId, String name, Map<String, Object> config) throws CatalogDBException;

    OpenCGAResult deleteConfig(String userId, String name) throws CatalogDBException;

    // Filter operations
    OpenCGAResult addFilter(String userId, UserFilter filter) throws CatalogDBException;

    OpenCGAResult updateFilter(String userId, String name, ObjectMap params) throws CatalogDBException;

    OpenCGAResult deleteFilter(String userId, String name) throws CatalogDBException;

    enum QueryParams implements QueryParam {
        ID("id", TEXT_ARRAY, ""),
        NAME("name", TEXT_ARRAY, ""),
        EMAIL("email", TEXT_ARRAY, ""),
        ORGANIZATION("organization", TEXT_ARRAY, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        ACCOUNT("account", TEXT_ARRAY, ""),
        SIZE("size", INTEGER_ARRAY, ""),
        QUOTA("quota", INTEGER_ARRAY, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"

        PROJECTS("projects", TEXT_ARRAY, ""),
        PROJECTS_UID("projects.uid", INTEGER_ARRAY, ""),
        PROJECTS_ID("projects.id", INTEGER_ARRAY, ""),
        PROJECT_NAME("projects.name", TEXT_ARRAY, ""),
        PROJECT_ORGANIZATION("projects.organization", TEXT_ARRAY, ""),
        PROJECT_STATUS("projects.status", TEXT_ARRAY, ""),

        TOOL_ID("tools.id", INTEGER_ARRAY, ""),
        TOOL_NAME("tools.name", TEXT_ARRAY, ""),
        TOOL_ALIAS("tools.alias", TEXT_ARRAY, ""),

        // TOCHECK: Pedro. Check whether login, logout makes sense.
        SESSIONS("sessions", TEXT_ARRAY, ""),
        SESSION_ID("sessions.id", TEXT_ARRAY, ""),
        SESSION_IP("sessions.ip", TEXT_ARRAY, ""),
        SESSION_LOGIN("sessions.login", TEXT_ARRAY, ""),
        SESSION_LOGOUT("sessions.logout", TEXT_ARRAY, ""),

        CONFIGS("configs", TEXT_ARRAY, ""),
        FILTERS("filters", TEXT_ARRAY, ""),
        FILTERS_ID("filters.id", TEXT, "");

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

    enum FilterParams implements QueryParam {
        ID("id", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        RESOURCE("resource", TEXT, ""),
        QUERY("query", TEXT, ""),
        OPTIONS("options", TEXT, "");

        private static Map<String, FilterParams> map;

        static {
            map = new LinkedMap();
            for (FilterParams params : FilterParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        FilterParams(String key, Type type, String description) {
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

        public static Map<String, FilterParams> getMap() {
            return map;
        }

        public static FilterParams getParam(String key) {
            return map.get(key);
        }
    }

    enum ToolQueryParams implements QueryParam {
        ID("id", TEXT, ""),
        ALIAS("alias", TEXT, ""),
        NAME("name", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        MANIFEST("manifest", TEXT, ""),
        RESULT("result", TEXT, ""),
        PATH("path", TEXT, ""),
        ACL_USER_ID("acl.userId", TEXT_ARRAY, ""),
        ACL_READ("acl.read", BOOLEAN, ""),
        ACL_WRITE("acl.write", BOOLEAN, ""),
        ACL_EXECUTE("acl.execute", BOOLEAN, ""),
        ACL_DELETE("acl.delete", BOOLEAN, "");

        private static Map<String, ToolQueryParams> map;
        static {
            map = new LinkedMap();
            for (ToolQueryParams params : ToolQueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        ToolQueryParams(String key, Type type, String description) {
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

        public static Map<String, ToolQueryParams> getMap() {
            return map;
        }

        public static ToolQueryParams getParam(String key) {
            return map.get(key);
        }
    }

    /**
     * Project methods moved to ProjectDBAdaptor
     * ***************************
     */

//    OpenCGAResult<Project> createProject(String userId, Project project, QueryOptions options) throws CatalogDBException;
//
//    boolean projectExists(int projectId);
//
//    OpenCGAResult<Project> getAllProjects(String userId, QueryOptions options) throws CatalogDBException;
//
//    OpenCGAResult<Project> getProject(int project, QueryOptions options) throws CatalogDBException;
//
//    OpenCGAResult<Integer> deleteProject(int projectId) throws CatalogDBException;
//
//    OpenCGAResult renameProjectAlias(int projectId, String newProjectName) throws CatalogDBException;
//
//    OpenCGAResult<Project> modifyProject(int projectId, ObjectMap parameters) throws CatalogDBException;
//
//    int getProjectId(String userId, String projectAlias) throws CatalogDBException;
//
//    String getProjectOwnerId(int projectId) throws CatalogDBException;
//
//    OpenCGAResult<AclEntry> getProjectAcl(int projectId, String userId) throws CatalogDBException;
//
//    OpenCGAResult setProjectAcl(int projectId, AclEntry newAcl) throws CatalogDBException;

}
