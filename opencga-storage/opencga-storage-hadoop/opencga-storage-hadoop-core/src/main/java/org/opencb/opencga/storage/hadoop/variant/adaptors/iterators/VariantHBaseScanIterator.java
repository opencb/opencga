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

package org.opencb.opencga.storage.hadoop.variant.adaptors.iterators;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created on 23/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHBaseScanIterator extends VariantDBIterator {

    private static final int POOL_SIZE = 4;
    private final Logger logger = LoggerFactory.getLogger(VariantHBaseScanIterator.class);
    private final Iterator<ResultScanner> resultScanners;
    private ResultScanner currentResultScanner;
    private Iterator<Result> resultIterator;
    private Iterator<Future<Variant>> buffer = Collections.emptyIterator();
    private final HBaseToVariantConverter<Result> converter;
    private long limit = Long.MAX_VALUE;
    private int count = 0;
    private ExecutorService threadPool;
    private AtomicLong timeConverting = new AtomicLong();
//    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
//            .namingPattern("variant-hbase-scan-convert-%s")
//            .build());

    public VariantHBaseScanIterator(Iterator<ResultScanner> resultScanners, VariantStorageMetadataManager scm,
                                    Query query, QueryOptions options, String unknownGenotype, List<String> formats,
                                    VariantQueryProjection selectElements)
            throws IOException {
        this.resultScanners = resultScanners;
        resultIterator = Collections.emptyIterator();
        converter = HBaseToVariantConverter.fromResult(scm)
                .setMutableSamplesPosition(false)
                .setStudyNameAsStudyId(options.getBoolean(HBaseToVariantConverter.STUDY_NAME_AS_STUDY_ID, true))
                .setSimpleGenotypes(options.getBoolean(HBaseToVariantConverter.SIMPLE_GENOTYPES, true))
                .setUnknownGenotype(unknownGenotype)
                .setSelectVariantElements(selectElements)
                .setIncludeIndexStatus(query.getBoolean(VariantQueryUtils.VARIANTS_TO_INDEX.key(), false))
                .setFormats(formats);
        setLimit(options.getLong(QueryOptions.LIMIT, Long.MAX_VALUE));
        threadPool = Executors.newFixedThreadPool(POOL_SIZE);
    }

    @Override
    public boolean hasNext() {
        if (count >= limit) {
            // Limit reached
            return false;
        }
        if (buffer.hasNext() || fetch(resultIterator::hasNext)) {
            return true;
        } else {
            nextResultSet();
            return fetch(resultIterator::hasNext);
        }
    }

    private void nextResultSet() {
        while (resultScanners.hasNext()) {
            if (currentResultScanner != null) {
                currentResultScanner.close();
            }
            currentResultScanner = resultScanners.next();
            resultIterator = currentResultScanner.iterator();
            if (fetch(resultIterator::hasNext)) {
                break;
            }
        }
    }

    @Override
    public Variant next() {
        if (count >= limit || !hasNext()) {
            throw new NoSuchElementException("Limit reached");
        }
        if (!buffer.hasNext()) {
            int i = (int) Math.min(50, limit - count);
            List<Future<Variant>> variants = new ArrayList<>(50);
            while (hasNext() && i > 0) {
                i--;
                Result result = fetch(resultIterator::next);
                variants.add(threadPool.submit(() -> {
                    long start = System.nanoTime();
                    Variant v = converter.convert(result);
                    timeConverting.addAndGet(System.nanoTime() - start);
                    return v;
                }));
            }
            buffer = variants.iterator();
        }
        count++;
        try {
            Future<Variant> next = buffer.next();
            return next.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        double timeConverting = getTimeConverting() / 1000000.0;
        logger.debug("Close variant iterator. Fetch = {}ms, Convert = {}ms (total)   ~{}ms/thread",
                getTimeFetching() / 1000000.0,
                timeConverting,
                timeConverting / POOL_SIZE);
        threadPool.shutdownNow();
        if (currentResultScanner != null) {
            currentResultScanner.close();
        }
    }

    @Override
    public long getTimeConverting() {
        return super.timeConverting + timeConverting.get();
    }

    @Override
    public int getCount() {
        return count;
    }

    public long getLimit() {
        return limit;
    }

    protected void setLimit(long limit) {
        this.limit = limit < 0 ? Long.MAX_VALUE : limit;
    }

    public void skip(int skip) {
        if (skip > 0) {
            while (hasNext() && skip > 0) {
                skip--;
                fetch(resultIterator::next);
            }
        }
    }
}
