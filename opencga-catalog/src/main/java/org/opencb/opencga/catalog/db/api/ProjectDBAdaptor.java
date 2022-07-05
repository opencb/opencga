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

package org.opencb.opencga.catalog.db.api;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by imedina on 08/01/16.
 */
public interface ProjectDBAdaptor extends Iterable<Project> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER_ARRAY, ""),
        UUID("uuid", TEXT, ""),
        NAME("name", TEXT_ARRAY, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        DESCRIPTION("description", TEXT_ARRAY, ""),
        ORGANIZATION("organization", TEXT, ""),
        CELLBASE("cellbase", OBJECT, ""),
        ORGANISM("organism", TEXT_ARRAY, ""),
        ORGANISM_SCIENTIFIC_NAME("organism.scientificName", TEXT, ""),
        ORGANISM_COMMON_NAME("organism.commonName", TEXT, ""),
        ORGANISM_ASSEMBLY("organism.assembly", TEXT, ""),
        CURRENT_RELEASE("currentRelease", INTEGER, ""),
        FQN("fqn", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_ID("internal.status.id", TEXT, ""),
        INTERNAL_STATUS_MSG("internal.status.msg", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        USER_ID("userId", TEXT, ""),
        INTERNAL_DATASTORES("internal.datastores", TEXT_ARRAY, ""),
        INTERNAL_DATASTORES_VARIANT("internal.datastores.variant", TEXT_ARRAY, ""),
        INTERNAL("internal", TEXT_ARRAY, ""),

        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"

        OWNER("owner", TEXT, ""),

        STUDIES("studies", TEXT_ARRAY, ""),
        STUDY("study", TEXT, ""), // For the project/search ws
        STUDY_UID("study.uid", INTEGER_ARRAY, ""),
        STUDY_ID("study.alias", TEXT_ARRAY, ""),

        // TOCHECK: Pedro. Check parameter user_others_id.
        ACL_USER_ID("acl.userId", TEXT_ARRAY, "");

        private static Map<String, QueryParams> map = new HashMap<>();

        static {
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


    default boolean exists(long projectId) throws CatalogDBException {
        return count(new Query(QueryParams.UID.key(), projectId)).getNumMatches() > 0;
    }

    default void checkId(long projectId) throws CatalogDBException {
        if (projectId < 0) {
            throw CatalogDBException.newInstance("Project id '{}' is not valid: ", projectId);
        }

        if (!exists(projectId)) {
            throw CatalogDBException.newInstance("Project id '{}' does not exist", projectId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> project, String userId) throws CatalogDBException;

    OpenCGAResult insert(Project project, String userId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Project> get(String userId, QueryOptions options) throws CatalogDBException;

    OpenCGAResult<Project> get(long project, QueryOptions options) throws CatalogDBException;

    OpenCGAResult incrementCurrentRelease(long projectId) throws CatalogDBException;

    long getId(String userId, String projectAlias) throws CatalogDBException;

    String getOwnerId(long projectId) throws CatalogDBException;


    default OpenCGAResult<Long> count() throws CatalogDBException {
        return count(new Query());
    }

    OpenCGAResult<Long> count(Query query) throws CatalogDBException;

    OpenCGAResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException, CatalogAuthorizationException;

    default OpenCGAResult distinct(String field) throws CatalogDBException {
        return distinct(new Query(), field);
    }

    OpenCGAResult distinct(Query query, String field) throws CatalogDBException;


    default OpenCGAResult stats() {
        return stats(new Query());
    }

    OpenCGAResult stats(Query query);


    OpenCGAResult<Project> get(Query query, QueryOptions options) throws CatalogDBException;

    OpenCGAResult<Project> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogParameterException;

    default List<OpenCGAResult<Project>> get(List<Query> queries, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(queries);
        List<OpenCGAResult<Project>> queryResults = new ArrayList<>(queries.size());
        for (Query query : queries) {
            queryResults.add(get(query, options));
        }
        return queryResults;
    }

    OpenCGAResult nativeGet(Query query, QueryOptions options) throws CatalogDBException;

    OpenCGAResult nativeGet(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException;

    default List<OpenCGAResult> nativeGet(List<Query> queries, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(queries);
        List<OpenCGAResult> queryResults = new ArrayList<>(queries.size());
        for (Query query : queries) {
            queryResults.add(nativeGet(query, options));
        }
        return queryResults;
    }

    OpenCGAResult<Project> update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException;

    OpenCGAResult delete(Project project) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult delete(Query query) throws CatalogDBException;

    default OpenCGAResult<Project> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult<Project> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    OpenCGAResult<Project> restore(long id, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException;


//    OpenCGAResult<Long> updateStatus(Query query, Status status) throws CatalogDBException;


    @Override
    default DBIterator<Project> iterator() {
        try {
            return iterator(new Query(), new QueryOptions());
        } catch (CatalogDBException e) {
            throw new RuntimeException(e);
        }
    }

    DBIterator<Project> iterator(Query query, QueryOptions options) throws CatalogDBException;

    default DBIterator nativeIterator() throws CatalogDBException {
        return nativeIterator(new Query(), new QueryOptions());
    }

    DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException;

    DBIterator<Project> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException;

    DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException;

    OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException;

    OpenCGAResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException;

    OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException;

    OpenCGAResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException;

    OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException;

    @Override
    default void forEach(Consumer action) {
        try {
            forEach(new Query(), action, new QueryOptions());
        } catch (CatalogDBException e) {
            throw new RuntimeException(e);
        }
    }

    void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException;

}
