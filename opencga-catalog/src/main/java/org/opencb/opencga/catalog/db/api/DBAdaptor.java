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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;

import java.util.List;
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

    default DataResult<Long> count() throws CatalogDBException {
        return count(new Query());
    }

    DataResult<Long> count(Query query) throws CatalogDBException;

    DataResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermission)
            throws CatalogDBException, CatalogAuthorizationException;

    default DataResult distinct(String field) throws CatalogDBException {
        return distinct(new Query(), field);
    }

    DataResult distinct(Query query, String field) throws CatalogDBException;


    default DataResult stats() {
        return stats(new Query());
    }

    DataResult stats(Query query);


    DataResult<T> get(Query query, QueryOptions options) throws CatalogDBException;

    DataResult<T> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

    DataResult nativeGet(Query query, QueryOptions options) throws CatalogDBException;

    DataResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

    DataResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException;

    DataResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException;

    DataResult delete(T id) throws CatalogDBException;

    DataResult delete(Query query) throws CatalogDBException;

    default DataResult delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default DataResult delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default DataResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default DataResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    DataResult restore(long id, QueryOptions queryOptions) throws CatalogDBException;

    DataResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException;


//    DataResult<Long> updateStatus(Query query, Status status) throws CatalogDBException;


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

//    DataResult<T> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

    DataResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException;

    DataResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException;

    DataResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException;

    DataResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException;

    DataResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
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
