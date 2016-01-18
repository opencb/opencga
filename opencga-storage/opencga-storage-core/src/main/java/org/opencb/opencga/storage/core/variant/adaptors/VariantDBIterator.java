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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.biodata.models.variant.Variant;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by jacobo on 9/01/15.
 */
public abstract class VariantDBIterator implements Iterator<Variant>, AutoCloseable {

    public static final EmptyVariantDBIterator EMPTY_ITERATOR = new EmptyVariantDBIterator();
    protected long timeFetching = 0;
    protected long timeConverting = 0;

    public long getTimeConverting() {
        return timeConverting;
    }

    public void setTimeFetching(long timeFetching) {
        this.timeFetching = timeFetching;
    }

    public long getTimeFetching() {
        return timeFetching;
    }

    public void setTimeConverting(long timeConverting) {
        this.timeConverting = timeConverting;
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
            timeFetching += System.nanoTime() - start;
        }
    }

    public static VariantDBIterator emptyIterator() {
        return EMPTY_ITERATOR;
    }

    static class EmptyVariantDBIterator extends VariantDBIterator {
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
        public void close() {
        }
    }

}
