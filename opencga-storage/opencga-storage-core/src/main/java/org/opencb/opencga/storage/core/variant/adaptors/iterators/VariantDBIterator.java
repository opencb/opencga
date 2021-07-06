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

import com.google.common.collect.Iterators;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class VariantDBIterator extends CloseableIterator<Variant> {

    public static final EmptyVariantDBIterator EMPTY_ITERATOR = new EmptyVariantDBIterator();
    public static final Comparator<Variant> VARIANT_COMPARATOR_AUTOMATIC = Comparator.comparing(Variant::getChromosome)
            .thenComparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(Variant::toString);
    public static final Comparator<Variant> VARIANT_COMPARATOR_FAST = (o1, o2) -> {
        VariantAvro v1 = o1.getImpl();
        VariantAvro v2 = o2.getImpl();
        int c = v1.getChromosome().compareTo(v2.getChromosome());
        if (c != 0) {
            return c;
        }
        c = v1.getStart().compareTo(v2.getStart());
        if (c != 0) {
            return c;
        }
        c = v1.getEnd().compareTo(v2.getEnd());
        if (c != 0) {
            return c;
        }
        c = v1.getReference().compareTo(v2.getReference());
        if (c != 0) {
            return c;
        }
        c = v1.getAlternate().compareTo(v2.getAlternate());
        if (c != 0) {
            return c;
        }
        if (o1.sameGenomicVariant(o2)) {
            return 0;
        } else {
            return o1.toString().compareTo(o2.toString());
        }
    };
    public static final Comparator<Variant> VARIANT_COMPARATOR = VARIANT_COMPARATOR_FAST;
    protected long timeFetching = 0;
    protected long timeConverting = 0;
    private final Logger logger = LoggerFactory.getLogger(VariantDBIterator.class);

    @Override
    public VariantDBIterator addCloseable(AutoCloseable closeable) {
        super.addCloseable(closeable);
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

    public final long getTime(TimeUnit timeUnit) {
        return getTimeConverting(timeUnit) + getTimeFetching(timeUnit);
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

    @Override
    public void forEachRemaining(Consumer<? super Variant> action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(next());
        }
        try {
            close();
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }
    }

    public DataResult<Variant> toDataResult() {
        List<Variant> result = new ArrayList<>();
        this.forEachRemaining(result::add);

        int numResults = result.size();
        int numTotalResults = -1; // Unknown numTotalResults

        return new DataResult<>((int) getTimeFetching(TimeUnit.MILLISECONDS), Collections.emptyList(), numResults, result, numTotalResults);
    }

    public VariantQueryResult<Variant> toDataResult(Map<String, List<String>> samples) {
        return new VariantQueryResult<>(toDataResult(), samples);
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

    public VariantDBIterator map(UnaryOperator<Variant> map) {
        return new MapperVariantDBIterator(this, map);
    }

    public VariantDBIterator mapBuffered(UnaryOperator<List<Variant>> map, int batchSize) {
        return new BufferedMappedVariantDBIterator(this, map, batchSize);
    }

    public VariantDBIterator localSkip(int skip) {
        Iterators.advance(this, skip);
        return this;
    }

    public VariantDBIterator localLimit(int limit) {
        return new LimitVariantDBIterator(this, limit);
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

}
