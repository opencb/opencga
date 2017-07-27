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
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private final ResultScanner resultScanner;
    private final GenomeHelper genomeHelper;
    private final Iterator<Result> iterator;
    private final HBaseToVariantConverter<Result> converter;
    private long limit = Long.MAX_VALUE;
    private long count = 0;

    public VariantHBaseScanIterator(ResultScanner resultScanner, GenomeHelper genomeHelper, StudyConfigurationManager scm,
                                    QueryOptions options, List<String> returnedSamplesList, String unknownGenotype, List<String> formats)
            throws IOException {
        this.resultScanner = resultScanner;
        this.genomeHelper = genomeHelper;
        iterator = resultScanner.iterator();
        converter = HBaseToVariantConverter.fromResult(genomeHelper, scm)
                .setMutableSamplesPosition(false)
                .setStudyNameAsStudyId(true)
                .setSimpleGenotypes(true)
                .setReadFullSamplesData(true)
                .setUnknownGenotype(unknownGenotype)
                .setReturnedSamples(returnedSamplesList)
                .setFormats(formats);
        setLimit(options.getLong(QueryOptions.LIMIT));
    }

    @Override
    public boolean hasNext() {
        return count < limit && fetch(iterator::hasNext);
    }

    @Override
    public Variant next() {
        if (count >= limit) {
            throw new NoSuchElementException("Limit reached");
        }
        count++;
        Result next = fetch(iterator::next);
        return convert(() -> converter.convert(next));
    }

    @Override
    public void close() throws Exception {
        super.close();
        logger.debug("Close variant iterator. Fetch = {}ms, Convert = {}ms",
                getTimeFetching() / 1000000.0, getTimeConverting() / 1000000.0);
        resultScanner.close();
    }

    public long getLimit() {
        return limit;
    }

    protected void setLimit(long limit) {
        this.limit = limit <= 0 ? Long.MAX_VALUE : limit;
    }
}
