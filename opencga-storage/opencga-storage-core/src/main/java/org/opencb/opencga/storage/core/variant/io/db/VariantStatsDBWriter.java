package org.opencb.opencga.storage.core.variant.io.db;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.ProgressLogger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
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
    private final StudyConfiguration studyConfiguration;
    private ProgressLogger progressLogger;
    private final AtomicLong numWrites = new AtomicLong();
    private final AtomicLong numStats = new AtomicLong();

    public VariantStatsDBWriter(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, QueryOptions options) {
        this.dbAdaptor = dbAdaptor;
        this.options = options;
        this.studyConfiguration = studyConfiguration;
    }

    @Override
    public boolean pre() {
        numWrites.set(0);
        numStats.set(0);
        return true;
    }

    @Override
    public boolean write(List<VariantStatsWrapper> batch) {
        QueryResult writeResult = dbAdaptor.updateStats(batch, studyConfiguration, options);

        numStats.addAndGet(batch.size());
        numWrites.addAndGet(writeResult.getNumResults());

        if (progressLogger != null) {
            progressLogger.increment(batch.size(), () -> ", up to position "
                    + batch.get(batch.size() - 1).getChromosome() + ":"
                    + batch.get(batch.size() - 1).getPosition());
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
