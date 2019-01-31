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

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.ProgressLogger;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created on 02/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsDBWriter implements DataWriter<VariantStatsWrapper> {

    private final VariantDBAdaptor dbAdaptor;
    private final QueryOptions options;
    private final StudyMetadata studyMetadata;
    private ProgressLogger progressLogger;
    private final AtomicLong numWrites = new AtomicLong();
    private final AtomicLong numStats = new AtomicLong();
    private final long timestamp;

    public VariantStatsDBWriter(VariantDBAdaptor dbAdaptor, StudyMetadata studyMetadata, QueryOptions options) {
        this.dbAdaptor = dbAdaptor;
        this.options = options;
        this.studyMetadata = studyMetadata;
        timestamp = System.currentTimeMillis();
    }

    @Override
    public boolean pre() {
        numWrites.set(0);
        numStats.set(0);
        return true;
    }

    @Override
    public boolean write(List<VariantStatsWrapper> batch) {
        QueryResult writeResult = dbAdaptor.updateStats(batch, studyMetadata.getName(), timestamp, options);

        numStats.addAndGet(batch.size());
        numWrites.addAndGet(writeResult.getNumResults());

        if (progressLogger != null) {
            progressLogger.increment(batch.size(), () -> ", up to position "
                    + batch.get(batch.size() - 1).getChromosome() + ':'
                    + batch.get(batch.size() - 1).getStart());
        }

        return true;
    }

    public VariantStatsDBWriter setProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }


    public long getNumWrites() {
        return numWrites.longValue();
    }

    public long getVariantStats() {
        return numStats.longValue();
    }
}
