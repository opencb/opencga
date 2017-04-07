package org.opencb.opencga.storage.core.variant.io.db;

import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.ProgressLogger;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.util.List;

/**
 * Created on 01/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationDBWriter implements ParallelTaskRunner.TaskWithException<VariantAnnotation, Object, IOException> {

    protected final VariantDBAdaptor dbAdaptor;
    protected final QueryOptions options;
    private ProgressLogger progressLogger;

    public VariantAnnotationDBWriter(VariantDBAdaptor dbAdaptor, QueryOptions options, ProgressLogger progressLogger) {
        this.dbAdaptor = dbAdaptor;
        this.options = options;
        this.progressLogger = progressLogger;
    }

    @Override
    public List<Object> apply(List<VariantAnnotation> list) throws IOException {
        QueryResult queryResult = dbAdaptor.updateAnnotations(list, options);
        logUpdate(list);
        return queryResult.getResult();
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
