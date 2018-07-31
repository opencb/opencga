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

package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created on 23/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHBaseScanIterator extends VariantDBIterator {

    private final Logger logger = LoggerFactory.getLogger(VariantHBaseScanIterator.class);
    private final Iterator<ResultScanner> resultScanners;
    private ResultScanner currentResultScanner;
    private final GenomeHelper genomeHelper;
    private Iterator<Result> resultIterator;
    private final HBaseToVariantConverter<Result> converter;
    private long limit = Long.MAX_VALUE;
    private int count = 0;

    public VariantHBaseScanIterator(Iterator<ResultScanner> resultScanners, GenomeHelper genomeHelper, StudyConfigurationManager scm,
                                    QueryOptions options, String unknownGenotype, List<String> formats,
                                    VariantQueryUtils.SelectVariantElements selectElements)
            throws IOException {
        this.resultScanners = resultScanners;
        this.genomeHelper = genomeHelper;
        resultIterator = Collections.emptyIterator();
        converter = HBaseToVariantConverter.fromResult(genomeHelper, scm)
                .setMutableSamplesPosition(false)
                .setStudyNameAsStudyId(options.getBoolean(HBaseToVariantConverter.STUDY_NAME_AS_STUDY_ID, true))
                .setSimpleGenotypes(options.getBoolean(HBaseToVariantConverter.SIMPLE_GENOTYPES, true))
                .setUnknownGenotype(unknownGenotype)
                .setSelectVariantElements(selectElements)
                .setFormats(formats);
        setLimit(options.getLong(QueryOptions.LIMIT));
    }

    @Override
    public boolean hasNext() {
        if (count >= limit) {
            // Limit reached
            return false;
        }
        if (fetch(resultIterator::hasNext)) {
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
        count++;
        Result next = fetch(resultIterator::next);
        return convert(() -> converter.convert(next));
    }

    @Override
    public void close() throws Exception {
        super.close();
        logger.debug("Close variant iterator. Fetch = {}ms, Convert = {}ms",
                getTimeFetching() / 1000000.0, getTimeConverting() / 1000000.0);
        if (currentResultScanner != null) {
            currentResultScanner.close();
        }
    }

    @Override
    public int getCount() {
        return count;
    }

    public long getLimit() {
        return limit;
    }

    protected void setLimit(long limit) {
        this.limit = limit <= 0 ? Long.MAX_VALUE : limit;
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
