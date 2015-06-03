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

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class CatalogDBAdaptor {

    public static interface FilterOption {
        String getKey();

        enum Type {
            NUMERICAL, TEXT, BOOLEAN
        }
        Type getType();
        String getDescription();
    }

    protected Logger logger;

    protected long startQuery(){
        return System.currentTimeMillis();
    }

    protected <T> QueryResult<T> endQuery(String queryId, long startTime, List<T> result) throws CatalogDBException {
        return endQuery(queryId, startTime, result, null, null);
    }

    protected <T> QueryResult<T> endQuery(String queryId, long startTime) throws CatalogDBException {
        return endQuery(queryId, startTime, Collections.<T>emptyList(), null, null);
    }

    protected <T> QueryResult<T> endQuery(String queryId, long startTime, QueryResult<T> result)
            throws CatalogDBException {
        long end = System.currentTimeMillis();
        result.setId(queryId);
        result.setDbTime((int)(end-startTime));
        if(result.getErrorMsg() != null && !result.getErrorMsg().isEmpty()){
            throw new CatalogDBException(result.getErrorMsg());
        }
        return result;
    }

    protected <T> QueryResult<T> endQuery(String queryId, long startTime, List<T> result,
                                          String errorMessage, String warnMessage) throws CatalogDBException {
        long end = System.currentTimeMillis();
        if(result == null){
            result = new LinkedList<>();
        }
        int numResults = result.size();
        QueryResult<T> queryResult = new QueryResult<>(queryId, (int) (end - startTime), numResults, numResults,
                warnMessage, errorMessage, result);
        if(errorMessage != null && !errorMessage.isEmpty()){
            throw new CatalogDBException(queryResult.getErrorMsg());
        }
        return queryResult;
    }


    /**
     * Says if the catalog database is ready to be used. If false, needs to be initialized
     */
    public abstract boolean isCatalogDBReady();

    /**
     * Initializes de Database with the initial structure.
     * @throws CatalogDBException   if there was any problem, or it was already initialized.
     */
    public abstract void initializeCatalogDB() throws CatalogDBException;

    public abstract void disconnect();

    public abstract CatalogUserDBAdaptor getCatalogUserDBAdaptor();

    public abstract CatalogStudyDBAdaptor getCatalogStudyDBAdaptor();

    public abstract CatalogFileDBAdaptor getCatalogFileDBAdaptor();

    public abstract CatalogSampleDBAdaptor getCatalogSampleDBAdaptor();

    public abstract CatalogJobDBAdaptor getCatalogJobDBAdaptor();
}
