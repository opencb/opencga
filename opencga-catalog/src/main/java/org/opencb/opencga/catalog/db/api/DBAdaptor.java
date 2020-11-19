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
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Created by imedina on 07/01/16.
 */
public interface DBAdaptor<T> extends Iterable<T> {

    /**
     * Include ACL list in the attributes field.
     */
    String INCLUDE_ACLS = "includeAcls";
    /**
     * Deprecated constant. Use SKIP_CHECK instead.
     */
    @Deprecated
    String FORCE = "force";

    default OpenCGAResult<Long> count() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query());
    }

    OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default OpenCGAResult<T> stats() {
        return stats(new Query());
    }

    OpenCGAResult<T> stats(Query query);


    OpenCGAResult<T> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default List<OpenCGAResult> nativeGet(List<Query> queries, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(queries);
        List<OpenCGAResult> queryResults = new ArrayList<>(queries.size());
        for (Query query : queries) {
            queryResults.add(nativeGet(query, options));
        }
        return queryResults;
    }

    OpenCGAResult<T> update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<T> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<T> delete(T id) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<T> delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default OpenCGAResult<T> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult<T> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult<T> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult<T> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    OpenCGAResult<T> restore(long id, QueryOptions queryOptions) throws CatalogDBException;

    OpenCGAResult<T> restore(Query query, QueryOptions queryOptions) throws CatalogDBException;

    @Override
    default DBIterator<T> iterator() {
        try {
            return iterator(new Query(), new QueryOptions());
        } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
            throw new RuntimeException(e);
        }
    }

    DBIterator<T> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default DBIterator nativeIterator() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(new Query(), new QueryOptions());
    }

    DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<T> rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<T> groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<T> groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    @Override
    default void forEach(Consumer action) {
        try {
            forEach(new Query(), action, new QueryOptions());
        } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
            throw new RuntimeException(e);
        }
    }

    void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

}
