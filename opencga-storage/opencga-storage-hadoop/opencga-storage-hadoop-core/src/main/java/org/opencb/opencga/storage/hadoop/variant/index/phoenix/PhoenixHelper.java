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

package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.schema.ConcurrentTableMutationException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final Configuration conf;
    private static Logger logger = LoggerFactory.getLogger(PhoenixHelper.class);

    public PhoenixHelper(Configuration conf) {
        this.conf = conf;
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
                Thread.interrupted();
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
            return StringUtils.isNotEmpty(schemaName) ? "\"" + schemaName + ":" + tableName + "\"" : "\"" + tableName + "\"";
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

    public void addMissingColumns(Connection con, String tableName, Collection<Column> newColumns, boolean oneCall, PTableType tableType)
            throws SQLException {
        Set<String> columns = getColumns(con, tableName, tableType).stream().map(Column::column).collect(Collectors.toSet());
        List<Column> missingColumns = newColumns.stream()
                .filter(column -> !columns.contains(column.column()))
                .collect(Collectors.toList());
        if (!missingColumns.isEmpty()) {
            logger.info("Adding missing columns: " + missingColumns);
            if (oneCall) {
                String sql = buildAlterAddColumns(tableName, missingColumns, true, tableType);
                logger.info(sql);
                execute(con, sql);
            } else {
                for (Column column : missingColumns) {
                    String sql = buildAlterAddColumn(tableName, column.column(), column.sqlType(), true, tableType);
                    logger.info(sql);
                    execute(con, sql);
                }
            }
        }
    }

    public String buildAlterDropColumns(String tableName, Collection<CharSequence> columns, boolean ifExists, PTableType tableType) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER ").append(tableType).append(' ').append(getEscapedFullTableName(tableType, tableName))
                .append(" DROP COLUMN ").append(ifExists ? "IF EXISTS " : "");
        Iterator<CharSequence> iterator = columns.iterator();
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
        logger.info("Dropping columns: " + columns);
        String sql = buildAlterDropColumns(tableName, columns, true, tableType);
        logger.info(sql);

        execute(con, sql);
    }

    public Connection newJdbcConnection() throws SQLException, ClassNotFoundException {
        return newJdbcConnection(conf);
    }

    public Connection newJdbcConnection(Configuration conf) throws SQLException, ClassNotFoundException {
        // Ensure PhoenixDriver is registered
        if (PhoenixDriver.INSTANCE == null) {
            throw new SQLException("Error registering PhoenixDriver");
        }
        logger.info("Opening connection to PhoenixDriver " + PhoenixDriver.INSTANCE);
        Connection connection = QueryUtil.getConnection(conf);
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        logger.info("Opened Phoenix DB connection {} called from {}", connection, Arrays.toString(stackTrace));
        return connection;
    }

    public static byte[] toBytes(Collection<?> collection, PArrayDataType arrayType) {
        PDataType pDataType = PDataType.arrayBaseType(arrayType);
        Object[] elements = collection.toArray();
        PhoenixArray phoenixArray = new PhoenixArray(pDataType, elements);
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
        try (ResultSet resultSet = con.getMetaData().getColumns(null, schema, table, null)) {

            List<Column> columns = new ArrayList<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString(PhoenixDatabaseMetaData.COLUMN_NAME);
                String typeName = resultSet.getString(PhoenixDatabaseMetaData.TYPE_NAME);
                columns.add(Column.build(columnName, PDataType.fromSqlTypeName(typeName)));
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
            if (!(o instanceof ColumnImpl)) {
                return false;
            }

            ColumnImpl column1 = (ColumnImpl) o;

            if (column != null ? !column.equals(column1.column) : column1.column != null) {
                return false;
            }
            return pDataType != null ? pDataType.equals(column1.pDataType) : column1.pDataType == null;

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
