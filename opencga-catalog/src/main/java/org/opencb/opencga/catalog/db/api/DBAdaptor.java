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
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.results.OpenCGAResult;

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


    OpenCGAResult<T> get(Query query, QueryOptions options) throws CatalogDBException;

    OpenCGAResult<T> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

    OpenCGAResult nativeGet(Query query, QueryOptions options) throws CatalogDBException;

    OpenCGAResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

    OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException;

    OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException;

    OpenCGAResult delete(T id) throws CatalogDBException;

    OpenCGAResult delete(Query query) throws CatalogDBException;

    default OpenCGAResult delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    @Deprecated
    default OpenCGAResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("");
    }

    OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException;

    OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException;


//    OpenCGAResult<Long> updateStatus(Query query, Status status) throws CatalogDBException;


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

//    OpenCGAResult<T> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException;

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
