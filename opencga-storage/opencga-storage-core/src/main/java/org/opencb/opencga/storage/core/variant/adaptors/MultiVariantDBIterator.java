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

package org.opencb.opencga.storage.core.variant.adaptors;

import com.google.common.base.Throwables;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;

/**
 * Iterates multiple variant iterators. Every time that voids one iterator, creates a new one using the iteratorFactory.
 *
 * Created on 04/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MultiVariantDBIterator extends VariantDBIterator {

    private final Iterator<Query> queryIterator;
    private final QueryOptions options;
    private final BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory;
    private VariantDBIterator variantDBIterator;
    // Total number of elements to return. Includes the skipped elements. limit + skip
    private final int maxResults;
    private final int skip;
    // Skip elements first time that hasNext or next is called.
    private boolean pendingSkip;
    // Count of returned results.
    private int numResults;
    private Logger logger = LoggerFactory.getLogger(MultiVariantDBIterator.class);
    private Query query;
    private int numQueries;

    /**
     * Creates a multi iterator given a iterator of variants. It will apply the query (if any) to all the variants in the iterator.
     * The iterator will group the variants in batches to split the query.
     *
     * @param variantsIterator  Iterator with all the variants to filter.
     * @param batchSize         Number of variants to use in each query
     * @param query             Base query.
     * @param options           Query options to be used with the iterator factory
     * @param iteratorFactory   Iterator factory. See {@link VariantDBAdaptor#iterator()}
     */
    public MultiVariantDBIterator(Iterator<?> variantsIterator, int batchSize,
                                  Query query, QueryOptions options,
                                  BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory) {
        this(buildQueryIterator(variantsIterator, batchSize, query), options, iteratorFactory);
    }

    /**
     * @param queryIterator   Query iterator. Provides queries to execute
     * @param options         Query options to be used with the iterator factory
     * @param iteratorFactory Iterator factory. See {@link VariantDBAdaptor#iterator()}
     */
    public MultiVariantDBIterator(Iterator<Query> queryIterator, QueryOptions options,
                                  BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory) {
        this.queryIterator = Objects.requireNonNull(queryIterator);
        this.options = options == null ? new QueryOptions() : new QueryOptions(options);
        this.iteratorFactory = Objects.requireNonNull(iteratorFactory);
        variantDBIterator = emptyIterator();

        int limit = this.options.getInt(QueryOptions.LIMIT, 0);
        skip = Math.max(0, this.options.getInt(QueryOptions.SKIP, 0));
        pendingSkip = skip != 0;

        if (limit <= 0) {
            limit = Integer.MAX_VALUE;
            maxResults = limit;
        } else {
            maxResults = limit + skip;
        }

        // Client side limit+skip. Remove from QueryOptions
        this.options.remove(QueryOptions.LIMIT);
        this.options.remove(QueryOptions.SKIP);

        query = null;
        numQueries = 0;
    }

    @Override
    public boolean hasNext() {
        init();
        if (numResults >= maxResults) {
            terminateIterator();
            return false;
        } else if (!fetch(variantDBIterator::hasNext)) {
            nextVariantIterator();
            return fetch(variantDBIterator::hasNext);
        } else {
            return true;
        }
    }

    /**
     * Get the next non-empty valid {@link #variantDBIterator}. If none, use {@link #emptyIterator()}
     */
    private void nextVariantIterator() {
        while (!fetch(variantDBIterator::hasNext) && fetch(queryIterator::hasNext)) {
            terminateIterator();
            query = fetch(queryIterator::next);
            numQueries++;
            QueryOptions options;
            if (maxResults != Integer.MAX_VALUE) {
                // We are expecting no more than maxResults - numResults
                // Modify the limit in the query
                options = new QueryOptions(this.options).append(QueryOptions.LIMIT, maxResults - numResults);
            } else {
                options = this.options;
            }
            variantDBIterator = fetch(() -> iteratorFactory.apply(query, options));
        }
        if (!fetch(variantDBIterator::hasNext)) {
            terminateIterator();
        }
    }

    private void terminateIterator() {
        // Accumulate statistics from previous iterator.
        timeFetching += variantDBIterator.getTimeFetching();
        timeConverting += variantDBIterator.getTimeConverting();
        try {
            variantDBIterator.close();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            variantDBIterator = emptyIterator();
        }
    }

    @Override
    public Variant next() {
        if (hasNext()) {
            Variant next = fetch(variantDBIterator::next);
            numResults++;
            return next;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public long getTimeConverting() {
        return timeConverting + variantDBIterator.getTimeConverting();
    }

    @Override
    public long getTimeFetching() {
        return timeFetching + variantDBIterator.getTimeFetching();
    }

    /**
     * Client side skip.
     */
    private void init() {
        if (pendingSkip) {
            // This lock avoids recursion
            pendingSkip = false;
            int skip = this.skip;
            while (skip > 0 && hasNext()) {
                next();
                skip--;
            }
            if (skip == 0) {
                logger.debug("Client side skip {} elements", this.skip);
            } else {
                logger.debug("Client side skip {} elements. Original skip {}", (this.skip - skip), this.skip);
            }
        }
    }

    private static Iterator<Query> buildQueryIterator(Iterator<?> variantsIterator, int batchSize, Query query) {
        Objects.requireNonNull(variantsIterator);
        return new Iterator<Query>() {
            @Override
            public boolean hasNext() {
                return variantsIterator.hasNext();
            }

            @Override
            public Query next() {
                Query newQuery;
                if (query == null) {
                    newQuery = new Query();
                } else {
                    newQuery = new Query(query);
                }
                List<Object> variants = new ArrayList<>(batchSize);
                do {
                    // Always execute "next" over variantsIterator, to fail if empty
                    variants.add(variantsIterator.next());
                } while (variantsIterator.hasNext() && variants.size() < batchSize);
                newQuery.append(VariantQueryParam.ID.key(), variants);
                return newQuery;
            }
        };
    }

    public Query getQuery() {
        return query;
    }

    public int getNumQueries() {
        return numQueries;
    }
}
