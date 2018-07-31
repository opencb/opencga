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

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class VariantDBIterator implements Iterator<Variant>, AutoCloseable {

    public static final EmptyVariantDBIterator EMPTY_ITERATOR = new EmptyVariantDBIterator();
    protected long timeFetching = 0;
    protected long timeConverting = 0;
    private List<AutoCloseable> closeables = new ArrayList<>();
    private final Logger logger = LoggerFactory.getLogger(VariantDBIterator.class);

    public VariantDBIterator addCloseable(AutoCloseable closeable) {
        this.closeables.add(closeable);
        return this;
    }

    public long getTimeConverting() {
        return timeConverting;
    }

    public final long getTimeConverting(TimeUnit timeUnit) {
        return timeUnit.convert(getTimeConverting(), TimeUnit.NANOSECONDS);
    }

    public final void setTimeFetching(long timeFetching) {
        this.timeFetching = timeFetching;
    }

    public long getTimeFetching() {
        return timeFetching;
    }

    public final long getTimeFetching(TimeUnit timeUnit) {
        return timeUnit.convert(getTimeFetching(), TimeUnit.NANOSECONDS);
    }

    public final void setTimeConverting(long timeConverting) {
        this.timeConverting = timeConverting;
    }

    /**
     * @return Number of returned variants
     */
    public abstract int getCount();

    public QueryResult<Variant> toQueryResult() {
        List<Variant> result = new ArrayList<>();
        this.forEachRemaining(result::add);
        try {
            close();
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }

        int numResults = result.size();
        int numTotalResults = -1; // Unknown numTotalResults

        return new QueryResult<>("", (int) getTimeFetching(TimeUnit.MILLISECONDS), numResults, numTotalResults, "", "", result);
    }

    public VariantQueryResult<Variant> toQueryResult(Map<String, List<String>> samples) {
        return new VariantQueryResult<>(toQueryResult(), samples);
    }


    protected interface TimeFunction<R, E extends Exception> {
        R call() throws E;
    }

    protected <R, E extends Exception> R convert(TimeFunction<R, E> converter) throws E {
        long start = System.nanoTime();
        try {
            return converter.call();
        } finally {
            timeConverting += System.nanoTime() - start;
        }
    }

    protected <R, E extends Exception> R fetch(TimeFunction<R, E> fetcher) throws E {
        long start = System.nanoTime();
        try {
            return fetcher.call();
        } finally {
            long delta = System.nanoTime() - start;
            if (TimeUnit.NANOSECONDS.toSeconds(delta) > 60) {
                logger.warn("Slow backend. Took " + (TimeUnit.NANOSECONDS.toMillis(delta) / 1000.0) + "s to fetch more data"
                        + " from iterator " + getClass());
            }
            this.timeFetching += delta;
        }
    }

    @Override
    public void close() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    public static VariantDBIterator emptyIterator() {
        return EMPTY_ITERATOR;
    }

    private static class EmptyVariantDBIterator extends VariantDBIterator {
        EmptyVariantDBIterator() {
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Variant next() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public int getCount() {
            return 0;
        }
    }

    public static VariantDBIterator wrapper(Iterator<Variant> variant) {
        return new VariantDBIteratorWrapper(variant);
    }

    private static class VariantDBIteratorWrapper extends VariantDBIterator {
        private final Iterator<Variant> iterator;
        private int count = 0;

        VariantDBIteratorWrapper(Iterator<Variant> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return fetch(iterator::hasNext);
        }

        @Override
        public Variant next() {
            count++;
            return fetch(iterator::next);
        }

        @Override
        public int getCount() {
            return count;
        }
    }

}
