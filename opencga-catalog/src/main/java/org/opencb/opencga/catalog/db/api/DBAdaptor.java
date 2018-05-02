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

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Created by imedina on 07/01/16.
 */
public interface DBAdaptor<T> extends Iterable<T> {

    /**
     * SKIP_CHECK is used when deleting a document. If SKIP_CHECK is set to false, the document will be deleted no matter if other
     * documents might depend on that one.
     */
    String SKIP_CHECK = "skipCheck";
    /**
     * Deprecated constant. Use SKIP_CHECK instead.
     */
    @Deprecated
    String FORCE = "force";

    default QueryResult<Long> count() throws CatalogDBException {
        return count(new Query());
    }

    QueryResult<Long> count(Query query) throws CatalogDBException;

    QueryResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException, CatalogAuthorizationException;

    default QueryResult distinct(String field) throws CatalogDBException {
        return distinct(new Query(), field);
    }

    QueryResult distinct(Query query, String field) throws CatalogDBException;


    default QueryResult stats() {
        return stats(new Query());
    }

    QueryResult stats(Query query);


    QueryResult<T> get(Query query, QueryOptions options) throws CatalogDBException;

    QueryResult<T> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

    default List<QueryResult<T>> get(List<Query> queries, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(queries);
        List<QueryResult<T>> queryResults = new ArrayList<>(queries.size());
        for (Query query : queries) {
            queryResults.add(get(query, options));
        }
        return queryResults;
    }

    QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException;

    default List<QueryResult> nativeGet(List<Query> queries, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(queries);
        List<QueryResult> queryResults = new ArrayList<>(queries.size());
        for (Query query : queries) {
            queryResults.add(nativeGet(query, options));
        }
        return queryResults;
    }

    QueryResult<T> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException;

    QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException;

    void delete(long id) throws CatalogDBException;

    void delete(Query query) throws CatalogDBException;

    @Deprecated
    default QueryResult<T> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default QueryResult<T> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    QueryResult<T> restore(long id, QueryOptions queryOptions) throws CatalogDBException;

    QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException;


//    QueryResult<Long> updateStatus(Query query, Status status) throws CatalogDBException;


    @Override
    default DBIterator<T> iterator() {
        try {
            return iterator(new Query(), new QueryOptions());
        } catch (CatalogDBException e) {
            throw new RuntimeException(e);
        }
    }

    DBIterator<T> iterator(Query query, QueryOptions options) throws CatalogDBException;

    default DBIterator nativeIterator() throws CatalogDBException {
        return nativeIterator(new Query(), new QueryOptions());
    }

    DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException;

    DBIterator<T> iterator(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

    default DBIterator nativeIterator(String user) throws CatalogDBException, CatalogAuthorizationException {
        return nativeIterator(new Query(), new QueryOptions(), user);
    }

    DBIterator nativeIterator(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

//    QueryResult<T> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

    QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException;

    QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException;

    QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException;

    QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException;

    QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
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
