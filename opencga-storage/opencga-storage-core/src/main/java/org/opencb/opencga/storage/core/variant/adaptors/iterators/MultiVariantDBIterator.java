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

package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import com.google.common.base.Throwables;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Iterates multiple variant iterators. Every time that voids one iterator, creates a new one using the iteratorFactory.
 *
 * Created on 04/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MultiVariantDBIterator extends VariantDBIterator {

    private final VariantQueryIterator queryIterator;
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
        this(new VariantQueryIterator(variantsIterator, query, batchSize), options, iteratorFactory);
    }

    /**
     * @param queryIterator   Query iterator. Provides queries to execute
     * @param options         Query options to be used with the iterator factory
     * @param iteratorFactory Iterator factory. See {@link VariantDBAdaptor#iterator()}
     */
    private MultiVariantDBIterator(VariantQueryIterator queryIterator, QueryOptions options,
                                   BiFunction<Query, QueryOptions, VariantDBIterator> iteratorFactory) {
        this.queryIterator = queryIterator;
        this.options = options == null ? new QueryOptions() : new QueryOptions(options);
        this.iteratorFactory = Objects.requireNonNull(iteratorFactory);
        variantDBIterator = emptyIterator();
        addCloseable(queryIterator);

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

    @Override
    public int getCount() {
        return numResults - skip;
    }

    /**
     * Get the next non-empty valid {@link #variantDBIterator}. If none, use {@link #emptyIterator()}
     */
    private void nextVariantIterator() {
        while (!fetch(variantDBIterator::hasNext) && fetch(queryIterator::hasNext)) {
            terminateIterator();
            numQueries++;
            QueryOptions options;
            int limit;
            if (maxResults != Integer.MAX_VALUE) {
                // We are expecting no more than maxResults - numResults
                // Modify the limit in the query
                limit = maxResults - numResults;
                options = new QueryOptions(this.options).append(QueryOptions.LIMIT, limit);
            } else {
                limit = Integer.MAX_VALUE;
                options = this.options;
            }
            query = fetch(() -> queryIterator.next(numResults, limit));

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

    public Query getQuery() {
        return query;
    }

    public int getNumQueries() {
        return numQueries;
    }

    private static class VariantQueryIterator implements Iterator<Query>, AutoCloseable {
        private static final int MAX_BATCH_SIZE = 5000;
        private static final int MIN_BATCH_SIZE = 100;

//        private static final int LAST_MATCH_PROBABILITY_WEIGHT = 5;
//        private static final int OLD_MATCH_PROBABILITY_WEIGHT = 1;

        private final Iterator<?> variantsIterator;
        private final Query query;
        private final int batchSize;
        // Probability that an element from the variants iterator matches with the second iterator
        private float matchProbability = 0.50f;
        // Count of returned results at the end of the previous query
//        private int lastQueryNumResults = 0;
        private int lastBatchSize;
        private int totalBatchSizeCount;
        //        private boolean firstBatch = true;
        private Logger logger = LoggerFactory.getLogger(VariantQueryIterator.class);

        VariantQueryIterator(Iterator<?> variantsIterator, Query query, int batchSize) {
            this.variantsIterator = Objects.requireNonNull(variantsIterator);
            this.query = query;
            this.batchSize = batchSize;
        }

        @Override
        public boolean hasNext() {
            return variantsIterator.hasNext();
        }

        @Override
        public Query next() {
            return next(batchSize);
        }

        public Query next(final int numResults, final int limit) {
            int batchSize;

            // Do not update `matchProbability` for the first batch
//            if (!firstBatch) {
//                int newResults = numResults - lastQueryNumResults;
//                float lastMatchProbability = newResults / (float) lastBatchSize;
//                logger.info("newResults = " + newResults);
//                logger.info("lastMatchProbability = " + lastMatchProbability);
//                // Calculate weighted arithmetic mean between previous and new probability
//                matchProbability = (matchProbability * OLD_MATCH_PROBABILITY_WEIGHT
//                        + lastMatchProbability * LAST_MATCH_PROBABILITY_WEIGHT)
//                        / (OLD_MATCH_PROBABILITY_WEIGHT + LAST_MATCH_PROBABILITY_WEIGHT);
//            }
//            firstBatch = false;
//            lastQueryNumResults = numResults;
            if (totalBatchSizeCount > 0) {
                matchProbability = numResults / (float) totalBatchSizeCount;
                matchProbability *= 0.8;
            }

            if (matchProbability == 0) {
                batchSize = MAX_BATCH_SIZE;
            } else {
                batchSize = Math.round(limit / matchProbability);

                if (batchSize > MAX_BATCH_SIZE) {
                    batchSize = MAX_BATCH_SIZE;
                } else if (batchSize < MIN_BATCH_SIZE) {
                    batchSize = MIN_BATCH_SIZE;
                }
            }
            logger.debug("numResults = " + numResults
                    + " totalBatchSizeCount = " + totalBatchSizeCount
                    + " limit = " + limit
                    + " lastBatchSize = " + lastBatchSize
                    + " matchProbability = " + matchProbability
                    + " batchSize = " + batchSize);

            totalBatchSizeCount += batchSize;
            lastBatchSize = batchSize;

            return next(batchSize);
        }

        public Query next(int batchSize) {
            Query newQuery;
            if (query == null) {
                newQuery = new Query();
            } else {
                newQuery = new Query(query);
            }
            StopWatch stopWatch = StopWatch.createStarted();
            List<Object> variants = new ArrayList<>(batchSize);
            do {
                // Always execute "next" over variantsIterator, to fail if empty
                variants.add(variantsIterator.next());
            } while (variantsIterator.hasNext() && variants.size() < batchSize);
            newQuery.append(VariantQueryParam.ID.key(), variants);
            logger.info("Get next query: " + stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000.0);
            return newQuery;
        }

        @Override
        public void close() throws Exception {
            if (variantsIterator instanceof AutoCloseable) {
                ((AutoCloseable) variantsIterator).close();
            }
        }
    }
}
