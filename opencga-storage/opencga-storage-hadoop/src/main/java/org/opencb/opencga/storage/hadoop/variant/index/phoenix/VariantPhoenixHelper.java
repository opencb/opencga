package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.ALTERNATE;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.BIOTYPE;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.CHROMOSOME;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.FULL_ANNOTATION;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.GENES;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.PHASTCONS;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.PHYLOP;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.POSITION;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.REFERENCE;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns.SO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 15/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixHelper {

    protected static Logger logger = LoggerFactory.getLogger(VariantPhoenixHelper.class);

    public enum Columns {
        CHROMOSOME("CHROMOSOME", PVarchar.INSTANCE),
        POSITION("POSITION", PUnsignedInt.INSTANCE),
        REFERENCE("REFERENCE", PVarchar.INSTANCE),
        ALTERNATE("ALTERNATE", PVarchar.INSTANCE),

        SO("SO", PIntegerArray.INSTANCE),
        GENES("GENES", PVarcharArray.INSTANCE),
        BIOTYPE("BIOTYPE", PVarcharArray.INSTANCE),
        TRANSCRIPTS("TRANSCRIPTS", PVarcharArray.INSTANCE),

        PHASTCONS("PHASTCONS", PFloat.INSTANCE),
        PHYLOP("PHYLOP", PFloat.INSTANCE),

        FULL_ANNOTATION("FULL_ANNOTATION", PVarchar.INSTANCE);

        private final String columnName;
        private final byte[] columnNameBytes;
        private PDataType pDataType;
        private final String sqlTypeName;

        Columns(String columnName, PDataType pDataType) {
            this.columnName = columnName;
            this.pDataType = pDataType;
            this.sqlTypeName = pDataType.getSqlTypeName();
            columnNameBytes = Bytes.toBytes(columnName);
        }

        public String column() {
            return columnName;
        }

        public byte[] bytes() {
            return columnNameBytes;
        }

        public PDataType getPDataType() {
            return pDataType;
        }

        public String sqlType() {
            return sqlTypeName;
        }

        @Override
        public String toString() {
            return columnName;
        }
    }


    private final GenomeHelper genomeHelper;

    public VariantPhoenixHelper(GenomeHelper genomeHelper) {
        this.genomeHelper = genomeHelper;
    }

    public Connection newJdbcConnection(HadoopCredentials credentials) throws SQLException {
//        return DriverManager.getConnection("jdbc:phoenix:" + credentials.getHost(), credentials.getUser(), credentials.getPass());
        return DriverManager.getConnection("jdbc:phoenix:" + credentials.getHost());
    }


    public void registerNewStudy(Connection con, String table, Integer studyId) throws SQLException {
        execute(con, buildCreateView(table));
        addView(con, table, studyId, PUnsignedInt.INSTANCE, VariantTableStudyRow.HOM_REF, VariantTableStudyRow.PASS_CNT,
                VariantTableStudyRow.CALL_CNT);
        addView(con, table, studyId, PUnsignedIntArray.INSTANCE, VariantTableStudyRow.HET_REF, VariantTableStudyRow.HOM_VAR,
                VariantTableStudyRow.OTHER, VariantTableStudyRow.NOCALL);
        con.commit();
    }

    private void addView(Connection con, String table, Integer studyId, PDataType<?> dataType, String ... columns) throws SQLException {
        for (String col : columns) {
            String sql = buildAlterViewAddColumn(table, VariantTableStudyRow.buildColumnKey(studyId, col), dataType.getSqlTypeName());
            execute(con, sql);
        }
    }

    private void execute(Connection con, String sql) throws SQLException {
        logger.debug(sql);
        con.createStatement().execute(sql);
    }

    public String buildCreateView(String tableName) {
        return buildCreateView(tableName, Bytes.toString(genomeHelper.getColumnFamily()));
    }

    public static String buildCreateView(String tableName, String columnFamily) {
        return "CREATE VIEW IF NOT EXISTS \"" + tableName + "\" " + "("
                + CHROMOSOME + " " + CHROMOSOME.sqlType() + " NOT NULL, "
                + POSITION + " " + POSITION.sqlType() + " NOT NULL, "
                + REFERENCE + " " + REFERENCE.sqlType() + " , "
                + ALTERNATE + " " + ALTERNATE.sqlType() + " , "
                + GENES + " " + GENES.sqlType() + " , "
                + BIOTYPE + " " + BIOTYPE.sqlType() + " , "
                + SO + " " + SO.sqlType() + " , "
                + PHYLOP + " " + PHYLOP.sqlType() + " , "
                + PHASTCONS + " " + PHASTCONS.sqlType() + " , "
                + FULL_ANNOTATION + " " + FULL_ANNOTATION.sqlType() + " "
                + "CONSTRAINT PK PRIMARY KEY (" + CHROMOSOME + ", " + POSITION + ", " + REFERENCE + ", " + ALTERNATE + ") " + ") "
                + "DEFAULT_COLUMN_FAMILY='" + columnFamily + "'";
    }

    public String buildAlterViewAddColumn(String tableName, String column, String type) {
        return buildAlterViewAddColumn(tableName, column, type, true);
    }

    public String buildAlterViewAddColumn(String tableName, String column, String type, boolean ifNotExists) {
        return "ALTER VIEW \"" + tableName + "\" ADD " + (ifNotExists ? "IF NOT EXISTS " : "") + "\"" + column + "\" " + type;
    }

    public static byte[] toBytes(Collection collection, PArrayDataType arrayType) {
        PDataType pDataType = PDataType.arrayBaseType(arrayType);
        Object[] elements = collection.toArray();
        PhoenixArray phoenixArray = new PhoenixArray(pDataType, elements);
        return arrayType.toBytes(phoenixArray);
    }

}
