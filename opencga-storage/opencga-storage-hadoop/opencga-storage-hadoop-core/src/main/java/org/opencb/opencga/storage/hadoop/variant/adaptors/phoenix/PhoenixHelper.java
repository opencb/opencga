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

package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.schema.ConcurrentTableMutationException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.*;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.core.common.ExceptionUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Created on 11/10/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PhoenixHelper {

    // Server offset, for server pagination, is only available in Phoenix4.8 or Phoenix4.7.0.2.5.0 (from HDP2.5.0)
    // See https://issues.apache.org/jira/browse/PHOENIX-2722
    public static final String PHOENIX_SERVER_OFFSET_AVAILABLE = "phoenix.server.offset.available";
    public static final int SLOW_OPERATION_MILLIS = 10 * 1000;

    private final Configuration conf;
    private static Logger logger = LoggerFactory.getLogger(PhoenixHelper.class);
    private static Method positionAtArrayElement;
    private TableName systemCatalog;

    public PhoenixHelper(Configuration conf) {
        this.conf = conf;
    }

    static {
        Class<?> decoder;
        try {
            decoder = Class.forName("org.apache.phoenix.schema.types.PArrayDataTypeDecoder");
        } catch (ClassNotFoundException e) {
            decoder = PArrayDataType.class;
        }
        try {
            positionAtArrayElement = decoder.getMethod("positionAtArrayElement",
                    ImmutableBytesWritable.class, Integer.TYPE, PDataType.class, Integer.class);
        } catch (NoSuchMethodException e) {
            // This should never happen!
            throw new RuntimeException(e);
        }
    }

    public static boolean positionAtArrayElement(ImmutableBytesWritable ptr, int arrayIndex, PDataType pDataType, Integer byteSize) {
//        return PArrayDataTypeDecoder.positionAtArrayElement(ptr, arrayIndex, instance, byteSize);
        try {
            Object o = positionAtArrayElement.invoke(null, ptr, arrayIndex, pDataType, byteSize);
            return o == null || (boolean) o;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean execute(Connection con, String sql) throws SQLException {
        return execute(con, sql, 5);
    }

    private boolean execute(Connection con, String sql, int retry) throws SQLException {
        logger.debug(sql);
        try (Statement statement = con.createStatement()) {
            return statement.execute(sql);
        } catch (ConcurrentTableMutationException e) {
            if (retry == 0) {
                throw e;
            }
            logger.debug("Catch " + e.getClass().getSimpleName());
            try {
                int millis = RandomUtils.nextInt(100, 1000);
                logger.debug("Sleeping " + millis + "ms");
                Thread.sleep(millis);
            } catch (InterruptedException interruption) {
                Thread.currentThread().interrupt();
            }
            return execute(con, sql, retry - 1);
        } catch (SQLException | RuntimeException e) {
            logger.error("Error executing '{}'", sql);
            throw e;
        }
    }

    public String explain(Connection con, String sql, BiConsumer<Logger, String> loggerMethod) throws SQLException {
        String explain = explain(con, sql);
        for (String s : explain.split("\n")) {
            loggerMethod.accept(logger, " | " +  s);
        }
        return explain;
    }

    public String explain(Connection con, String sql) throws SQLException {
        if (!sql.startsWith("explain") && !sql.startsWith("EXPLAIN")) {
            sql = "EXPLAIN " + sql;
        }
        try (Statement statement = con.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) { // Close both - Statement and RS to make sure
            return QueryUtil.getExplainPlan(resultSet);
        }
    }

    public String buildAlterAddColumn(String tableName, String column, String type, boolean ifNotExists, PTableType tableType) {
        return "ALTER " + tableType.toString() + " " + getEscapedFullTableName(tableType, tableName)
                + " ADD " + (ifNotExists ? "IF NOT EXISTS " : "") + "\"" + column + "\" " + type;
    }

    protected String getEscapedFullTableName(PTableType tableType, String fullTableName) {
        return getEscapedFullTableName(tableType, fullTableName, conf);
    }

    protected static String getEscapedFullTableName(PTableType tableType, String fullTableName, Configuration conf) {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
        String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);

        if (isNamespaceMappingEnabled(tableType, conf)) {
            return SchemaUtil.getEscapedTableName(schemaName, tableName);
        } else {
            if (StringUtils.isEmpty(schemaName) || schemaName.equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR)) {
                return "\"" + tableName + "\"";
            } else {
                return "\"" + schemaName + ":" + tableName + "\"";
            }
        }
    }

    public String buildAlterAddColumns(String tableName, Collection<Column> columns, boolean ifNotExists, PTableType tableType) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER ").append(tableType).append(" ").append(getEscapedFullTableName(tableType, tableName))
                .append(" ADD ").append(ifNotExists ? "IF NOT EXISTS " : "");
        Iterator<Column> iterator = columns.iterator();
        while (iterator.hasNext()) {
            Column column = iterator.next();
            sb.append("\"").append(column.column()).append("\" ").append(column.sqlType());
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public String buildDropTable(String tableName, PTableType tableType, boolean ifExists, boolean cascade) {
        StringBuilder sb = new StringBuilder().append("DROP ").append(tableType).append(' ');
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(getEscapedFullTableName(tableType, tableName));
        if (cascade) {
            sb.append(" CASCADE");
        }
        return sb.toString();
    }

    public void dropTable(Connection con, String tableName, PTableType tableType, boolean ifExists, boolean cascade) throws SQLException {
        execute(con, buildDropTable(tableName, tableType, ifExists, cascade));
    }

    public void addMissingColumns(Connection con, String tableName, Collection<Column> newColumns, PTableType tableType)
            throws SQLException {
        LinkedHashSet<String> columns = getColumns(con, tableName, tableType).stream()
                .map(Column::column)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        addMissingColumns(con, tableName, newColumns, tableType, columns);
    }

    public void addMissingColumns(Connection con, String tableName, Collection<Column> newColumns, PTableType tableType,
                                  Set<String> alreadyDefinedColumns)
            throws SQLException {
        Set<Column> missingColumns = newColumns.stream()
                .filter(column -> !alreadyDefinedColumns.contains(column.column()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (!missingColumns.isEmpty()) {
            logger.info("Adding missing columns: " + missingColumns);
            List<Column> missingColumnsList = new ArrayList<>(missingColumns);
            // Run alter table in batches
            int batchSize = 5000;
            for (List<Column> batch : BatchUtils.splitBatches(missingColumnsList, batchSize)) {
                String sql = buildAlterAddColumns(tableName, batch, true, tableType);
                logger.info(sql);
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                execute(con, sql);
                if (stopWatch.getTime() > SLOW_OPERATION_MILLIS) {
                    logger.warn("Slow ALTER " + tableType.name() + ". Took " + TimeUtils.durationToString(stopWatch));
                }
            }
        }
    }

    public String buildAlterDropColumns(String tableName, Collection<CharSequence> columns, boolean ifExists, PTableType tableType) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER ").append(tableType).append(' ').append(getEscapedFullTableName(tableType, tableName))
                .append(" DROP COLUMN ").append(ifExists ? "IF EXISTS " : "");
        Iterator<CharSequence> iterator = new HashSet<>(columns).iterator();
        while (iterator.hasNext()) {
            sb.append('"').append(iterator.next()).append('"');
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public void dropColumns(Connection con, String tableName, Collection<CharSequence> columns, PTableType tableType)
            throws SQLException {

        Set<String> existingColumns = getColumns(con, tableName, tableType)
                .stream()
                .map(Column::column)
                .collect(Collectors.toSet());
        columns = new HashSet<>(columns);
        // Remove non existing columns
        columns.removeIf(c -> !existingColumns.contains(c.toString()));
        if (columns.isEmpty()) {
            // No column exists
            logger.info("Nothing to drop! Columns not defined in table '{}' : {}", tableName, columns);
            return;
        } else {
            logger.info("Dropping columns from table '{}' : {}", tableName, columns);
        }

        // Run alter table in batches
        int batchSize = 5000;
        for (List<CharSequence> batch : BatchUtils.splitBatches(new ArrayList<>(columns), batchSize)) {
            String sql = buildAlterDropColumns(tableName, batch, true, tableType);
            logger.info(sql);

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            execute(con, sql);
            if (stopWatch.getTime() > SLOW_OPERATION_MILLIS) {
                logger.warn("Slow ALTER DROP " + tableType.name() + ". Took " + TimeUtils.durationToString(stopWatch));
            }
        }
    }

    public Connection openJdbcConnection() throws SQLException, ClassNotFoundException {
        // Ensure PhoenixDriver is registered
        if (PhoenixDriver.INSTANCE == null) {
            throw new SQLException("Error registering PhoenixDriver");
        }
//        logger.info("Opening connection to PhoenixDriver");
        Connection connection = QueryUtil.getConnection(conf);
        List<StackTraceElement> stackTrace = ExceptionUtils.getOpencbStackTrace();
        logger.info("Open Phoenix DB connection #{} {} called from {}",
                GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getTotalSum(),
                connection, stackTrace);
        return connection;
    }

    private void closeJdbcConnection(Connection connection) throws SQLException {
        if (connection != null) {
            logger.info("Close Phoenix connection {} called from {}", connection, ExceptionUtils.getOpencbStackTrace());
            connection.close();
            logger.info("Global Phoenix Connections opened: #{}",
                    GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getTotalSum());
        }
    }

    public static byte[] toBytes(Collection<?> collection, PArrayDataType arrayType) {
        return toBytes(collection.toArray(), arrayType);
    }

    public static byte[] toBytes(Object[] elements, PArrayDataType arrayType) {
        PDataType pDataType = PDataType.arrayBaseType(arrayType);
        PhoenixArray phoenixArray = PArrayDataType.instantiatePhoenixArray(pDataType, elements);
        return arrayType.toBytes(phoenixArray);
    }

    public void createIndexes(Connection con, PTableType tableType, String tableName, List<Index> indices, boolean async)
            throws SQLException {
        for (PhoenixHelper.Index index : indices) {
            String sql = createIndexSql(tableType, tableName, index, async);
            execute(con, sql);
        }
    }

    public void createLocalIndex(Connection con, PTableType tableType, String tableName, String indexName, List<String> columns,
                                 List<String> include)
            throws SQLException {
        String sql = createIndexSql(PTable.IndexType.LOCAL, tableType, tableName, indexName, columns, include, false);
        execute(con, sql);
    }

    public void createGlobalIndex(Connection con, PTableType tableType, String indexName, String tableName, List<String> columns,
                                  List<String> include)
            throws SQLException {
        String sql = createIndexSql(PTable.IndexType.GLOBAL, tableType, tableName, indexName, columns, include, false);
        execute(con, sql);
    }

    public String createIndexSql(PTableType tableType, String tableName, Index index, boolean async) {
        return createIndexSql(index.indexType, tableType, tableName, index.indexName, index.columns, index.include, async);
    }

    public String createIndexSql(PTable.IndexType type, PTableType tableType, String tableName, String indexName,
                                 List<String> columns, List<String> include, boolean async) {
        Objects.requireNonNull(indexName);
        Objects.requireNonNull(tableName);
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns can not be empty");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (type.equals(PTable.IndexType.LOCAL)) {
            sb.append(" LOCAL");
        }
        sb.append(" INDEX IF NOT EXISTS ");

        sb.append(SchemaUtil.getEscapedArgument(indexName))
                .append(" ON ").append(getEscapedFullTableName(tableType, tableName)).append(" ( ");
        for (Iterator<String> iterator = columns.iterator(); iterator.hasNext();) {
            String column = iterator.next();
            sb.append(SchemaUtil.getEscapedFullColumnName(column));
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(" )");
        if (include != null && !include.isEmpty()) {
            sb.append(" INCLUDE(");
            for (Iterator<String> iterator = include.iterator(); iterator.hasNext();) {
                String column = iterator.next();
                sb.append(SchemaUtil.getEscapedFullColumnName(column));
                if (iterator.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(" )");
        }
        if (async) {
            sb.append(" ASYNC");
        }
        String sql = sb.toString();
        logger.info("Creating index: {}", sql);
        return sql;
    }

    public boolean tableExists(Connection con, String table) throws SQLException {
        try {
            PhoenixRuntime.getTable(con, table);
            return true;
        } catch (TableNotFoundException ignore) {
            return false;
        }
    }

    private TableName getSystemCatalogTable(HBaseManager hBaseManager) throws IOException {
        if (systemCatalog != null) {
            return systemCatalog;
        }
        if (hBaseManager.tableExists("SYSTEM.CATALOG")) {
            systemCatalog = TableName.valueOf("SYSTEM.CATALOG");
        } else if (hBaseManager.tableExists("SYSTEM:CATALOG")) {
            systemCatalog = TableName.valueOf("SYSTEM:CATALOG");
        }
        return systemCatalog;
    }

    public List<Column> getColumns(HBaseManager hBaseManager, String fullTableName, PTableType tableType, List<String> columnsFilter)
            throws IOException {
        TableName systemCatalogTable = getSystemCatalogTable(hBaseManager);
        if (systemCatalogTable == null) {
            return Collections.emptyList();
        }
        String tenant = "";
        String schema;
        String table;
        if (isNamespaceMappingEnabled(tableType, conf)) {
            schema = SchemaUtil.getSchemaNameFromFullName(fullTableName);
            table = SchemaUtil.getTableNameFromFullName(fullTableName);
        } else {
            schema = null;
            table = fullTableName;
        }
        return hBaseManager.act(systemCatalogTable.getNameAsString(), systemCatalog -> {
            List<Column> columns = new ArrayList<>();
            List<Get> gets = new ArrayList<>(columnsFilter.size());
            for (String column : columnsFilter) {
                String row = tenant + "\\x00"
                        + (schema == null ? "" : schema) + "\\x00"
                        + table + "\\x00"
                        + column + "\\x00"
                        + GenomeHelper.COLUMN_FAMILY;
                byte[] rowKey = Bytes.toBytesBinary(row);
                gets.add(new Get(rowKey).addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.DATA_TYPE_BYTES));
            }
            Result[] get = systemCatalog.get(gets);
            for (int i = 0; i < get.length; i++) {
                Result result = get[i];
                if (result != null && !result.isEmpty()) {
//                    StringUtils.splitByWholeSeparatorPreserveAllTokens(Bytes.toStringBinary(result.getRow()), "\\x00")
                    Cell c = result.rawCells()[0];
                    Integer dataType = (Integer) PInteger.INSTANCE.toObject(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                    columns.add(Column.build(columnsFilter.get(i), PDataType.fromTypeId(dataType)));
                }
            }
//            logger.info("Columns from " + systemCatalogTable + " : " + columns);
            return columns;
        });

    }

    public List<Column> getColumns(Connection con, String fullTableName, PTableType tableType) throws SQLException {
        String schema;
        String table;
        if (isNamespaceMappingEnabled(tableType, conf)) {
            schema = SchemaUtil.getSchemaNameFromFullName(fullTableName);
            table = SchemaUtil.getTableNameFromFullName(fullTableName);
        } else {
            schema = null;
            table = fullTableName;
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try (ResultSet resultSet = con.getMetaData().getColumns(null, schema, table, null)) {
            List<Column> columns = new ArrayList<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString(PhoenixDatabaseMetaData.COLUMN_NAME);
                String typeName = resultSet.getString(PhoenixDatabaseMetaData.TYPE_NAME);
                columns.add(Column.build(columnName, PDataType.fromSqlTypeName(typeName)));
            }
            if (stopWatch.getTime() > SLOW_OPERATION_MILLIS) {
                logger.warn("Slow read columns from Phoenix. Took " + TimeUtils.durationToString(stopWatch));
            }
            return columns;
        }
    }

    public static boolean isNamespaceMappingEnabled(PTableType tableType, Configuration conf) {
        // There is an issue with namespace mapping for VIEW tables.
        // This is fixed in version 4.12
        // Do not map the namespace for VIEW tables.
        //
        // See : https://issues.apache.org/jira/browse/PHOENIX-3944
        if (tableType.equals(PTableType.VIEW)
                && PhoenixDriver.INSTANCE.getMajorVersion() == 4
                && PhoenixDriver.INSTANCE.getMinorVersion() < 12) {
            return false;
        } else {
            return SchemaUtil.isNamespaceMappingEnabled(tableType, new ReadOnlyProps(conf.iterator()));
        }
    }

    public interface Column {

        /**
         * @return Full column name, including namespace
         */
        default String fullColumn() {
            return GenomeHelper.COLUMN_FAMILY + ":" + column();
        }

        /**
         * @return Column name
         */
        String column();

        byte[] bytes();

        PDataType getPDataType();

        String sqlType();

        boolean nullable();

        static Column build(String column, PDataType pDataType) {
            return new ColumnImpl(column, pDataType, false);
        }

        static Column build(byte[] column, PDataType pDataType) {
            return new ColumnImpl(column, pDataType, false);
        }

        static Column build(String column, PDataType pDataType, boolean nullable) {
            return new ColumnImpl(column, pDataType, nullable);
        }

        default ColumnInfo toColumnInfo() {
            return new ColumnInfo(column(), getPDataType().getSqlType());
        }
    }

    private static class ColumnImpl implements Column {

        private final String column;
        private final PDataType pDataType;
        private boolean nullable;

        ColumnImpl(String column, PDataType pDataType, boolean nullable) {
            this(column, Bytes.toBytes(column), pDataType, nullable);
        }

        ColumnImpl(byte[] bytes, PDataType pDataType, boolean nullable) {
            this(Bytes.toString(bytes), bytes, pDataType, nullable);
        }

        ColumnImpl(String column, byte[] bytes, PDataType pDataType, boolean nullable) {
            this.bytes = bytes;
            this.column = column;
            this.pDataType = pDataType;
            this.nullable = nullable;
        }

        private byte[] bytes;

        @Override
        public String column() {
            return column;
        }

        @Override
        public byte[] bytes() {
            return bytes;
        }

        @Override
        public PDataType getPDataType() {
            return pDataType;
        }

        @Override
        public String sqlType() {
            return pDataType.getSqlTypeName();
        }

        @Override
        public boolean nullable() {
            return nullable;
        }

        @Override
        public String toString() {
            return column;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Column)) {
                return false;
            }

            Column column1 = (Column) o;

            if (column != null ? !column.equals(column1.column()) : column1.column() != null) {
                return false;
            }
            return pDataType != null ? pDataType.equals(column1.getPDataType()) : column1.getPDataType() == null;

        }

        @Override
        public int hashCode() {
            int result = column != null ? column.hashCode() : 0;
            result = 31 * result + (pDataType != null ? pDataType.hashCode() : 0);
            return result;
        }
    }

    public static class Index {
        private final String indexName;
        private final PTable.IndexType indexType;
        private final List<String> columns;
        private final List<String> include;

        public Index(String indexName, PTable.IndexType indexType, Column... columns) {
            this.indexName = indexName;
            this.indexType = indexType;
            this.columns = Arrays.stream(columns).map(Column::column).collect(Collectors.toList());
            this.include = null;
        }

        public Index(TableName table, PTable.IndexType indexType, List<?> columns, List<?> include) {
            this(table.getNameAsString().replaceAll("[:\\\\.]", "_").toUpperCase() + "_" + columns
                            .stream()
                            .map(Object::toString)
                            .collect(Collectors.joining("_"))
                            .replaceAll("[\"\\[\\]]", "") + "_IDX",
                    indexType, columns, include);
        }

        public Index(String indexName, PTable.IndexType indexType, List<?> columns, List<?> include) {
            this.indexName = indexName;
            this.indexType = indexType;

            this.columns = columns.stream()
                    .map(o -> o instanceof Column ? ((Column) o).column() : o.toString())
                    .collect(Collectors.toList());
            this.include = include.stream()
                    .map(o -> o instanceof Column ? ((Column) o).column() : o.toString())
                    .collect(Collectors.toList());
        }
    }
}
