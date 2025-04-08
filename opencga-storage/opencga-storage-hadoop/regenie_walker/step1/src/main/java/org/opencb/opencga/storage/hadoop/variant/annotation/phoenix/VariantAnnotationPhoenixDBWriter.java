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

package org.opencb.opencga.storage.hadoop.variant.annotation.phoenix;

import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.io.db.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToPhoenixConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 26/10/16.
 */
public class VariantAnnotationPhoenixDBWriter extends VariantAnnotationDBWriter {

    private final Connection connection;
    private final VariantAnnotationUpsertExecutor upsertExecutor;
    private final VariantAnnotationToPhoenixConverter converter;
    private final boolean closeConnection;
    protected static Logger logger = LoggerFactory.getLogger(VariantAnnotationPhoenixDBWriter.class);
    private final VariantPhoenixSchemaManager schemaManager;

    public VariantAnnotationPhoenixDBWriter(VariantHadoopDBAdaptor dbAdaptor, QueryOptions options, String variantTable,
                                            Connection jdbcConnection, boolean closeConnection) {
        this(dbAdaptor, options, variantTable, null, jdbcConnection, closeConnection);
    }

    public VariantAnnotationPhoenixDBWriter(VariantHadoopDBAdaptor dbAdaptor, QueryOptions options, String variantTable,
                                            ProgressLogger progressLogger, Connection jdbcConnection, boolean closeConnection) {
        super(dbAdaptor, options, progressLogger);
        this.connection = jdbcConnection;

        this.closeConnection = closeConnection;
        int currentAnnotationId = dbAdaptor.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getId();
        this.converter = new VariantAnnotationToPhoenixConverter(GenomeHelper.COLUMN_FAMILY_BYTES, currentAnnotationId);
        List<PhoenixHelper.Column> columns = new ArrayList<>();
        Collections.addAll(columns, VariantPhoenixSchema.VariantColumn.values());
        columns.addAll(VariantPhoenixSchema.getHumanPopulationFrequenciesColumns());

        this.upsertExecutor = new VariantAnnotationUpsertExecutor(connection,
                VariantPhoenixSchema.getEscapedFullTableName(variantTable, dbAdaptor.getConfiguration()), columns);
        schemaManager = new VariantPhoenixSchemaManager(dbAdaptor);
    }

    @Override
    public synchronized void pre() throws StorageEngineException {
        schemaManager.registerAnnotationColumns();
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
    public void post() throws IOException, SQLException {
        upsertExecutor.close();
        if (closeConnection) {
            logger.info("Close Phoenix connection " + connection);
            connection.close();
        }
        schemaManager.close();
    }
}
