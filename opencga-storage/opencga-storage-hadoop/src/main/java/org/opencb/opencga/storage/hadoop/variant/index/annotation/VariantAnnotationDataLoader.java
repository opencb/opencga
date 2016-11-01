package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.phoenix.util.SchemaUtil;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.core.common.ProgressLogger;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by jacobo on 26/10/16.
 */
public class VariantAnnotationDataLoader implements ParallelTaskRunner.TaskWithException<VariantAnnotation, Void, IOException> {

    private final Connection connection;
    private final ProgressLogger progressLogger;
    private final VariantAnnotationUpsertExecutor upsertExecutor;
    private final VariantAnnotationToHBaseConverter converter;
    private String variantTable;
    private HBaseManager hBaseManager;
    private GenomeHelper genomeHelper;

    public VariantAnnotationDataLoader(Connection connection, String variantTable, GenomeHelper genomeHelper) {
        this(connection, variantTable, genomeHelper, null);
    }

    public VariantAnnotationDataLoader(Connection connection, String variantTable, GenomeHelper genomeHelper,
                                       ProgressLogger progressLogger) {
        this.connection = connection;
        this.progressLogger = progressLogger;
        this.genomeHelper = genomeHelper;
        this.converter = new VariantAnnotationToHBaseConverter(this.genomeHelper);
        this.variantTable = variantTable;
        List<PhoenixHelper.Column> columns = new ArrayList<>();
        Collections.addAll(columns, VariantPhoenixHelper.VariantColumn.values());
        columns.addAll(VariantPhoenixHelper.getHumanPopulationFrequenciesColumns());

        this.upsertExecutor = new VariantAnnotationUpsertExecutor(this.connection,
                SchemaUtil.getEscapedFullTableName(variantTable), columns);
        hBaseManager = genomeHelper.getHBaseManager();

    }

    @Override
    public synchronized void pre() {
        VariantPhoenixHelper variantPhoenixHelper = new VariantPhoenixHelper(genomeHelper);
        try {
            //TODO: Read population frequencies columns from StudyConfiguration ?
            variantPhoenixHelper.getPhoenixHelper().addMissingColumns(connection, variantTable,
                    VariantPhoenixHelper.getHumanPopulationFrequenciesColumns(), true);
            variantPhoenixHelper.updateAnnotationColumns(connection, variantTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<Void> apply(List<VariantAnnotation> variantAnnotationList) throws IOException {
        Iterable<Map<PhoenixHelper.Column, ?>> records = converter.apply(variantAnnotationList);

        upsertExecutor.execute(records);

//        List<Put> puts = new ArrayList<>(variantAnnotationList.size());
//        for (Map<PhoenixHelper.Column, ?> record : records) {
//            Put put = converter.buildPut(record, column -> column.column().startsWith(VariantPhoenixHelper.POPULATION_FREQUENCY_PREFIX));
//            if (put != null) {
//                puts.add(put);
//            }
//        }
//
//        hBaseManager.act(variantTable, table -> {
//            table.put(puts);
//        });

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
