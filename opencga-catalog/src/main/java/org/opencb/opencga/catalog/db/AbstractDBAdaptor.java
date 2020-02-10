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

package org.opencb.opencga.catalog.db;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractDBAdaptor {

    protected final Logger logger;

    protected AbstractDBAdaptor(Logger logger) {
        this.logger = logger;
    }

    protected long startQuery() {
        return System.currentTimeMillis();
    }

    protected <T> OpenCGAResult<T> endQuery(long startTime, DBIterator<T> iterator) {
        List<T> results = new LinkedList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return endQuery(startTime, results, iterator.getNumMatches(), new ArrayList<>());
    }

    protected <T> OpenCGAResult<T> endQuery(long startTime, List<T> result) {
        return endQuery(startTime, result, -1, new ArrayList<>());
    }

    protected <T> OpenCGAResult<T> endQuery(long startTime, List<T> result, long numMatches) {
        return endQuery(startTime, result, numMatches, new ArrayList<>());
    }

    protected <T> OpenCGAResult<T> endQuery(long startTime, DataResult<T> result) {
        result.setTime((int) (System.currentTimeMillis() - startTime));
        logger.trace("DbTime: {}, numResults: {}, numMatches: {}", result.getTime(), result.getNumResults(), result.getNumMatches());
        return new OpenCGAResult<>(result);
    }

    protected <T> OpenCGAResult<T> endQuery(long startTime, List<T> results, long numMatches, List<Event> events) {
        long end = System.currentTimeMillis();
        if (results == null) {
            results = new LinkedList<>();
        }
        int numResults = results.size();
        OpenCGAResult<T> result = new OpenCGAResult<>((int) (end - startTime), events, numResults, results, numMatches, new ObjectMap());
        logger.trace("DbTime: {}, numResults: {}, numMatches: {}", result.getTime(), result.getNumResults(), result.getNumMatches());
        return result;
    }

    protected <T> OpenCGAResult<T> endWrite(long startTime, long numMatches, long numUpdated, List<Event> events) {
        long end = System.currentTimeMillis();
        return new OpenCGAResult<>((int) (end - startTime), events, numMatches, 0, numUpdated, 0, new ObjectMap());
    }

    protected <T> OpenCGAResult<T> endWrite(long startTime, long numMatches, long numInserted, long numUpdated, long numDeleted,
                                         List<Event> events) {
        long end = System.currentTimeMillis();
        return new OpenCGAResult<>((int) (end - startTime), events, numMatches, numInserted, numUpdated, numDeleted, new ObjectMap());
    }

    protected <T> OpenCGAResult<T> endWrite(long startTime, DataResult result) {
        long end = System.currentTimeMillis();
        return new OpenCGAResult<>((int) (end - startTime), result.getEvents(), result.getNumMatches(), result.getNumInserted(),
                result.getNumUpdated(), result.getNumDeleted(), new ObjectMap());
    }

    protected void checkParameter(Object param, String name) throws CatalogDBException {
        if (param == null) {
            throw new CatalogDBException("Error: parameter '" + name + "' is null");
        }
        if (param instanceof String) {
            if (param.equals("") || param.equals("null")) {
                throw new CatalogDBException("Error: parameter '" + name + "' is empty or it values 'null");
            }
        }
    }

    public interface FilterOption {
        String getKey();

        Type getType();

        String getDescription();

        enum Type {
            /**
             * Accepts a list of comma separated numerical conditions, where the value must match in, at least, one of this.
             * The accepted operators are: [<, <=, >, >=, =, , !=]
             * <p>
             * Example:
             * getAllFiles ( {size : "<200000" } )
             * getAllFiles ( {jobId : "32,33,34" } )
             * </p>
             */
            NUMERICAL,
            /**
             * Accepts a list of comma separated text conditions, where the value must match in, at least, one of this.
             * The accepted operators are: [<, <=, >, >=, =, , !=, ~, =~, !=~],
             * where [~,=~] implements a "LIKE" with regular expression and [!=~, !~] implements a "NOT LIKE"
             * and [<, <=, >, >=] are lexicographical operations
             * <p>
             * Example:
             * getAllFiles ( { bioformat : "VARIANT," } )
             * getAllSamples ( { name : "~SAMP_00[0-9]*"} )
             * </p>
             */
            TEXT,
            /**
             * Accepts a boolean condition.
             * <p>
             * Example:
             * getAllFiles ( { acl.userId : "user1", acl.write : "false" } )
             * </p>
             */
            BOOLEAN
        }
    }

}
