package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.phoenix.util.SchemaUtil;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.ProgressLogger;
import org.opencb.opencga.storage.core.variant.io.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
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
public class VariantAnnotationPhoenixDBWriter extends VariantAnnotationDBWriter {

    private final Connection connection;
    private final VariantAnnotationUpsertExecutor upsertExecutor;
    private final VariantAnnotationToHBaseConverter converter;
    private final boolean closeConnection;
    private String variantTable;
    private GenomeHelper genomeHelper;

    public VariantAnnotationPhoenixDBWriter(VariantHadoopDBAdaptor dbAdaptor, QueryOptions options, String variantTable,
                                            Connection jdbcConnection, boolean closeConnection) {
        this(dbAdaptor, options, variantTable, null, jdbcConnection, closeConnection);
    }

    public VariantAnnotationPhoenixDBWriter(VariantHadoopDBAdaptor dbAdaptor, QueryOptions options, String variantTable,
                                            ProgressLogger progressLogger, Connection jdbcConnection, boolean closeConnection) {
        super(dbAdaptor, options, progressLogger);
        this.connection = jdbcConnection;

        this.genomeHelper = dbAdaptor.getGenomeHelper();
        this.closeConnection = closeConnection;
        this.converter = new VariantAnnotationToHBaseConverter(genomeHelper);
        this.variantTable = variantTable;
        List<PhoenixHelper.Column> columns = new ArrayList<>();
        Collections.addAll(columns, VariantPhoenixHelper.VariantColumn.values());
        columns.addAll(VariantPhoenixHelper.getHumanPopulationFrequenciesColumns());

        this.upsertExecutor = new VariantAnnotationUpsertExecutor(connection, SchemaUtil.getEscapedFullTableName(variantTable), columns);
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
    public List<Object> apply(List<VariantAnnotation> variantAnnotationList) throws IOException {
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

        logUpdate(variantAnnotationList);

        return Collections.emptyList();
    }

    @Override
    public void post() {
        try {
            upsertExecutor.close();
            if (closeConnection) {
                connection.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
