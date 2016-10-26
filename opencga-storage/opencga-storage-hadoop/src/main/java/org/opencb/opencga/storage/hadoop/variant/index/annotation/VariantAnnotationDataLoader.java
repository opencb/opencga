package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.phoenix.util.SchemaUtil;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.core.common.ProgressLogger;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 26/10/16.
 */
public class VariantAnnotationDataLoader implements ParallelTaskRunner.Task<VariantAnnotation, Void> {

    private final Connection connection;
    private final ProgressLogger progressLogger;
    private final VariantAnnotationUpsertExecutor upsertExecutor;
    private final VariantAnnotationToHBaseConverter converter;

    public VariantAnnotationDataLoader(Connection connection, String variantTable, GenomeHelper genomeHelper) {
        this(connection, variantTable, genomeHelper, null);
    }

    public VariantAnnotationDataLoader(Connection connection, String variantTable, GenomeHelper genomeHelper,
                                       ProgressLogger progressLogger) {
        this.connection = connection;
        this.progressLogger = progressLogger;
        this.converter = new VariantAnnotationToHBaseConverter(genomeHelper);
        this.upsertExecutor = new VariantAnnotationUpsertExecutor(this.connection, SchemaUtil.getEscapedFullTableName(variantTable));
    }

    @Override
    public List<Void> apply(List<VariantAnnotation> variantAnnotationList) {
        Iterable<Map<PhoenixHelper.Column, ?>> records = converter.apply(variantAnnotationList);
        upsertExecutor.execute(records);

        if (progressLogger != null) {
            progressLogger.increment(variantAnnotationList.size(),
                    () -> {
                        VariantAnnotation last = variantAnnotationList.get(variantAnnotationList.size() - 1);
                        return ", up to position " + last.getChromosome() + ":" + last.getStart() + ":"
                                + last.getReference() + ":" + last.getAlternate();
                    });
        }

        return Collections.emptyList();
    }

    @Override
    public void post() {
        try {
            upsertExecutor.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
