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

import com.google.common.base.Function;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.UpsertExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper.Column;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_SQL_COUNTER;

/**
 * Created on 24/10/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationUpsertExecutor extends UpsertExecutor<Map<Column, ?>, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(VariantAnnotationUpsertExecutor.class);
    private final List<Column> columnList;

    public VariantAnnotationUpsertExecutor(Connection conn, String tableName) {
        this(conn, tableName, Arrays.stream(VariantPhoenixHelper.VariantColumn.values()).collect(Collectors.toList()));
    }

    public VariantAnnotationUpsertExecutor(Connection conn, String tableName, List<Column> columnList) {
        this(conn, tableName, columnList, new UpsertListener<Map<Column, ?>>() {
            @Override
            public void upsertDone(long upsertCount) {
//                System.out.println("upsertCount = " + upsertCount);
            }

            @Override
            public void errorOnRecord(Map<Column, ?> columnMap, Throwable e) {
                LOG.error("ERROR LOADING: " + columnMap, e);
                if (e instanceof Exception) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public VariantAnnotationUpsertExecutor(Connection conn, String tableName, List<Column> columnList,
                                           UpsertListener<Map<Column, ?>> upsertListener) {
        super(conn, tableName, columnList.stream().map(Column::toColumnInfo).collect(Collectors.toList()), upsertListener);
        this.columnList = columnList;

        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            // Impossible?
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void execute(Map<Column, ?> map) {
        try {
            for (int fieldIndex = 0; fieldIndex < columnList.size(); fieldIndex++) {
                Column column = columnList.get(fieldIndex);
                Object sqlValue;
                sqlValue = map.get(column);
                if (sqlValue != null) {
                    if (column.getPDataType().isArrayType()) {
                        if (sqlValue instanceof Collection) {
                            sqlValue = toArray(column.getPDataType(), (Collection) sqlValue);
                        } else {
                            throw new IllegalArgumentException("Column " + column + " is not a collection " + sqlValue);
                        }
                    }
                    preparedStatement.setObject(fieldIndex + 1, sqlValue);
                } else {
                    preparedStatement.setNull(fieldIndex + 1, dataTypes.get(fieldIndex).getSqlType());
                }
            }

            preparedStatement.execute();
            LOG.debug("preparedStatement.getUpdateCount() = " + preparedStatement.getUpdateCount());
            upsertListener.upsertDone(++upsertCount);

        } catch (RuntimeException | SQLException e) {
            if (LOG.isDebugEnabled()) {
                // Even though this is an error we only log it with debug logging because we're notifying the
                // listener, and it can do its own logging if needed
                LOG.debug("Error on variant " + map, e);
            }
            upsertListener.errorOnRecord(map, e);
        }
    }

    private Array toArray(PDataType elementDataType, Collection<?> input) {
        if (elementDataType.isArrayType()) {
            elementDataType = PDataType.arrayBaseType(elementDataType);
        }
        return new PhoenixArray(elementDataType, input.toArray(new Object[input.size()]));
    }


    @Override
    protected Function<Object, Object> createConversionFunction(PDataType dataType) {
//        return input -> dataType.toObject(input, dataType);
        return input -> input;
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new IOException(e);
        }
        LOG.debug("GLOBAL_MUTATION_SQL_COUNTER = " + GLOBAL_MUTATION_SQL_COUNTER.getMetric().getTotalSum());
    }

//    void putDynamicColumns(Map<Column, ?> map) {
//        EnumSet<VariantPhoenixHelper.VariantColumn> variantColumns = EnumSet.allOf(VariantPhoenixHelper.VariantColumn.class);
//
//    }

}
