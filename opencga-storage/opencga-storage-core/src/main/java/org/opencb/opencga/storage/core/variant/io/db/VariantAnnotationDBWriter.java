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

import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created on 01/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationDBWriter implements Task<VariantAnnotation, Object> {

    protected final VariantDBAdaptor dbAdaptor;
    protected final QueryOptions options;
    private ProgressLogger progressLogger;
    private final long timestamp;

    public VariantAnnotationDBWriter(VariantDBAdaptor dbAdaptor, QueryOptions options, ProgressLogger progressLogger) {
        this.dbAdaptor = dbAdaptor;
        this.options = options;
        this.progressLogger = progressLogger;
        timestamp = System.currentTimeMillis();
    }

    @Override
    public List<Object> apply(List<VariantAnnotation> list) throws IOException {
        DataResult writeResult = dbAdaptor.updateAnnotations(list, timestamp, options);
        logUpdate(list);
        return Collections.singletonList(writeResult);
    }

    protected void logUpdate(List<VariantAnnotation> list) {
        if (progressLogger != null) {
            progressLogger.increment(list.size(), () -> {
                VariantAnnotation annotation = list.get(list.size() - 1);
                return ", up to position "
                        + annotation.getChromosome() + ":"
                        + annotation.getStart() + ":"
                        + annotation.getReference() + ":"
                        + annotation.getAlternate();
            });
        }
    }

    public VariantAnnotationDBWriter setProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }
}
