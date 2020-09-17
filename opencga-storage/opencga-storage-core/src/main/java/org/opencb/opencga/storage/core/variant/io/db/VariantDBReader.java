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

package org.opencb.opencga.storage.core.variant.io.db;

import com.google.common.base.Throwables;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jmmut on 3/03/15.
 */
public class VariantDBReader implements VariantReader {
    private VariantIterable iterable;
    private Query query;
    private QueryOptions options;
    private VariantDBIterator iterator;
    private long timeFetching = 0;
    private long timeConverting = 0;

    protected static Logger logger = LogManager.getLogger(VariantDBReader.class);

    public VariantDBReader(VariantIterable iterable, Query query, QueryOptions options) {
        this.iterable = iterable;
        this.query = query;
        this.options = options;
    }

    public VariantDBReader(VariantDBIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public List<String> getSampleNames() {
        return null;
    }

    @Override
    public VariantFileMetadata getVariantFileMetadata() {
//        return getVariantFileMetadata(-1);
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean open() {
        if (iterator != null) {
            return true;
        }
        QueryOptions iteratorQueryOptions;
        if (options != null) { //Parse query options
            iteratorQueryOptions = new QueryOptions(options);
        } else {
            iteratorQueryOptions = new QueryOptions();
        }

        iterator = iterable.iterator(query, iteratorQueryOptions);
        return iterator != null;
    }


    @Override
    public List<Variant> read(int batchSize) {

        StopWatch watch = StopWatch.createStarted();
        List<Variant> variants = new ArrayList<>(batchSize);
        while (variants.size() < batchSize && iterator.hasNext()) {
            variants.add(iterator.next());
        }

        long newTimeFetching = iterator.getTimeFetching(TimeUnit.MILLISECONDS);
        long newTimeConverting = iterator.getTimeConverting(TimeUnit.MILLISECONDS);

        logger.debug("another batch of {} elements read. time: {}ms", variants.size(), watch.getTime());
        logger.debug("time splitted: fetch = {}ms, convert = {}ms",
                newTimeFetching - this.timeFetching,
                newTimeConverting - this.timeConverting);

        this.timeFetching = newTimeFetching;
        this.timeConverting = newTimeConverting;

        return variants;
    }

    @Override
    public boolean close() {
        try {
            iterator.close();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return true;
    }

    public long getTimeConverting(TimeUnit timeUnit) {
        return timeUnit.convert(timeConverting, TimeUnit.MILLISECONDS);
    }

    public long getTimeFetching(TimeUnit timeUnit) {
        return timeUnit.convert(timeFetching, TimeUnit.MILLISECONDS);
    }

}
