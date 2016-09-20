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

package org.opencb.opencga.storage.core.variant.io;

import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Created by jmmut on 3/03/15.
 */
public class VariantDBReader implements VariantReader {
    private StudyConfiguration studyConfiguration;
    private VariantDBAdaptor variantDBAdaptor;
    private Query query;
    private QueryOptions options;
    private VariantDBIterator iterator;
    protected static Logger logger = LoggerFactory.getLogger(VariantDBReader.class);

    public VariantDBReader(VariantDBAdaptor variantDBAdaptor, Query query, QueryOptions options) {
        this(null, variantDBAdaptor, query, options);
    }

    public VariantDBReader(StudyConfiguration studyConfiguration, VariantDBAdaptor variantDBAdaptor, Query query, QueryOptions options) {
        this.studyConfiguration = studyConfiguration;
        this.variantDBAdaptor = variantDBAdaptor;
        this.query = query;
        this.options = options;
    }

    public VariantDBReader() {
    }

    @Override
    public List<String> getSampleNames() {
        return studyConfiguration != null ? new LinkedList<>(studyConfiguration.getSampleIds().keySet()) : null;
    }

    @Override
    public String getHeader() {
        return getSource().getMetadata().get(VariantFileUtils.VARIANT_FILE_HEADER).toString();
    }

    public VariantSource getSource() {
        return getSource(-1);
    }

    public VariantSource getSource(int fileId) {
        Iterator<VariantSource> sourceIterator;
        try {
            Query sourceQuery = new Query();
            if (fileId >= 0) {
                sourceQuery.put(VariantSourceDBAdaptor.VariantSourceQueryParam.FILE_ID.key(), fileId);
            }
            sourceIterator = variantDBAdaptor.getVariantSourceDBAdaptor()
                    .iterator(sourceQuery, new QueryOptions());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (sourceIterator.hasNext()) {
            return sourceIterator.next();
        }
        return null;
    }

    @Override
    public boolean open() {
        QueryOptions iteratorQueryOptions;
        if (options != null) { //Parse query options
            iteratorQueryOptions = new QueryOptions(options);
        } else {
            iteratorQueryOptions = new QueryOptions();
        }

        // TODO rethink this way to refer to the Variant fields (through DBObjectToVariantConverter)
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternate", "reference", "sourceEntries");
        iteratorQueryOptions.putIfAbsent(QueryOptions.INCLUDE, include);   // do not overwrite in case a "include" was already specified

        iterator = variantDBAdaptor.iterator(query, iteratorQueryOptions);
        return iterator != null;
    }


    @Override
    public List<Variant> read(int batchSize) {

        long start = System.currentTimeMillis();
        List<Variant> variants = new ArrayList<>(batchSize);
        while (variants.size() < batchSize && iterator.hasNext()) {
            variants.add(iterator.next());
        }
        logger.debug("another batch of {} elements read. time: {}ms", variants.size(), System.currentTimeMillis() - start);
        logger.debug("time splitted: fetch = {}ms, convert = {}ms", iterator.getTimeFetching(), iterator.getTimeConverting());

        iterator.setTimeConverting(0);
        iterator.setTimeFetching(0);

        return variants;
    }
}
