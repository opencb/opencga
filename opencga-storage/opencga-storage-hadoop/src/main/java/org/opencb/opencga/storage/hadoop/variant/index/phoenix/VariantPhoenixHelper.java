package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.QueryUtil;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;

/**
 * Created on 15/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixHelper {

    protected static Logger logger = LoggerFactory.getLogger(VariantPhoenixHelper.class);

    public interface Column {
        String column();

        byte[] bytes();

        PDataType getPDataType();

        String sqlType();

        static Column build(String column, PDataType pDataType) {
            return new Column() {

                private byte[] bytes = Bytes.toBytes(column);

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
                public String toString() {
                    return column;
                }
            };
        }
    }

    public enum VariantColumn implements Column {
        CHROMOSOME("CHROMOSOME", PVarchar.INSTANCE),
        POSITION("POSITION", PUnsignedInt.INSTANCE),
        REFERENCE("REFERENCE", PVarchar.INSTANCE),
        ALTERNATE("ALTERNATE", PVarchar.INSTANCE),

        SO("SO", PIntegerArray.INSTANCE),
        GENES("GENES", PVarcharArray.INSTANCE),
        BIOTYPE("BIOTYPE", PVarcharArray.INSTANCE),
        TRANSCRIPTS("TRANSCRIPTS", PVarcharArray.INSTANCE),
        TRANSCRIPTION_FLAGS("FLAGS", PVarcharArray.INSTANCE),
        GENE_TRAITS_NAME("GT_NAME", PVarcharArray.INSTANCE),
        GENE_TRAITS_ID("GT_ID", PVarcharArray.INSTANCE),
        PROTEIN_KEYWORDS("PROT_KW", PVarcharArray.INSTANCE),
        DRUG("DRUG", PVarcharArray.INSTANCE),

        //Protein substitution scores
        POLYPHEN("POLYPHEN", PFloatArray.INSTANCE),
//        POLYPHEN_DESC("POLYPHEN_DESC", PVarcharArray.INSTANCE),
        SIFT("SIFT", PFloatArray.INSTANCE),
//        SIFT_DESC("SIFT_DESC", PVarcharArray.INSTANCE),

        //Conservation Scores
        PHASTCONS("PHASTCONS", PFloat.INSTANCE),
        PHYLOP("PHYLOP", PFloat.INSTANCE),

        FULL_ANNOTATION("FULL_ANNOTATION", PVarchar.INSTANCE);

        private final String columnName;
        private final byte[] columnNameBytes;
        private PDataType pDataType;
        private final String sqlTypeName;

        VariantColumn(String columnName, PDataType pDataType) {
            this.columnName = columnName;
            this.pDataType = pDataType;
            this.sqlTypeName = pDataType.getSqlTypeName();
            columnNameBytes = Bytes.toBytes(columnName);
        }

        @Override
        public String column() {
            return columnName;
        }

        @Override
        public byte[] bytes() {
            return columnNameBytes;
        }

        @Override
        public PDataType getPDataType() {
            return pDataType;
        }

        @Override
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

    public Connection newJdbcConnection(HBaseCredentials credentials) throws SQLException {
      return DriverManager.getConnection("jdbc:phoenix:" + credentials.getHost()); // this one was working before hbase version
//        return DriverManager.getConnection("jdbc:phoenix:" + credentials.getHost(), credentials.getUser(), credentials.getPass());
    }

    public Connection newJdbcConnection(Configuration conf) throws SQLException, ClassNotFoundException {
        String connectionUrl = QueryUtil.getConnectionUrl(new Properties(), conf);
        logger.debug("connectionUrl = " + connectionUrl);
        return DriverManager.getConnection(connectionUrl);
    }

    public void updateAnnotationFields(Connection con, String tableName) throws SQLException {
        VariantColumn[] annotColumns = new VariantColumn[]{GENES, BIOTYPE, SO, POLYPHEN, SIFT, PHYLOP, PHASTCONS, FULL_ANNOTATION};
        for (VariantColumn column : annotColumns) {
            String sql = buildAlterViewAddColumn(tableName, column.column(), column.sqlType(), true);
            execute(con, sql);
        }
    }

    public void registerNewStudy(Connection con, String table, Integer studyId) throws SQLException {
        execute(con, buildCreateView(table));
        addView(con, table, studyId, PUnsignedInt.INSTANCE, VariantTableStudyRow.HOM_REF, VariantTableStudyRow.PASS_CNT,
                VariantTableStudyRow.CALL_CNT);
        addView(con, table, studyId, PUnsignedIntArray.INSTANCE, VariantTableStudyRow.HET_REF, VariantTableStudyRow.HOM_VAR,
                VariantTableStudyRow.OTHER, VariantTableStudyRow.NOCALL);
        addView(con, table, studyId, PVarbinary.INSTANCE, VariantTableStudyRow.COMPLEX, VariantTableStudyRow.FILTER_OTHER);
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
        StringBuilder sb = new StringBuilder().append("CREATE VIEW IF NOT EXISTS \"").append(tableName).append("\" ").append("(");
        //.append(CHROMOSOME).append(" ").append(CHROMOSOME.sqlType()).append(" NOT NULL, ").append(POSITION).append(" ").append(POSITION.sqlType()).append(" NOT NULL, ").append(REFERENCE).append(" ").append(REFERENCE.sqlType()).append(" , ").append(ALTERNATE).append(" ").append(ALTERNATE.sqlType()).append(" , ").append(SO).append(" ").append(SO.sqlType()).append(" , ").append(GENES).append(" ").append(GENES.sqlType()).append(" , ").append(BIOTYPE).append(" ").append(BIOTYPE.sqlType()).append(" , ").append(TRANSCRIPTION_FLAGS).append(" ").append(TRANSCRIPTION_FLAGS.sqlType()).append(" , ").append(GENE_TRAITS_NAME).append(" ").append(GENE_TRAITS_NAME.sqlType()).append(" , ").append(GENE_TRAITS_ID).append(" ").append(GENE_TRAITS_ID.sqlType()).append(" , ").append(DRUG).append(" ").append(DRUG.sqlType()).append(" , ").append(POLYPHEN).append(" ").append(POLYPHEN.sqlType()).append(" , ").append(SIFT).append(" ").append(SIFT.sqlType()).append(" , ").append(PHYLOP).append(" ").append(PHYLOP.sqlType()).append(" , ").append(PHASTCONS).append(" ").append(PHASTCONS.sqlType()).append(" , ").append(FULL_ANNOTATION).append(" ").append(FULL_ANNOTATION.sqlType())
        for (VariantColumn variantColumn : VariantColumn.values()) {
            switch (variantColumn) {
                case CHROMOSOME:
                case POSITION:
                    sb.append(" ").append(variantColumn).append(" ").append(variantColumn.sqlType()).append(" NOT NULL , ");
                    break;
                default:
                    sb.append(" ").append(variantColumn).append(" ").append(variantColumn.sqlType()).append(" , ");
                    break;
            }
        }

        return sb.append(" ").append("CONSTRAINT PK PRIMARY KEY (").append(CHROMOSOME).append(", ").append(POSITION).append(", ").append(REFERENCE).append(", ").append(ALTERNATE).append(") ").append(") ").append("DEFAULT_COLUMN_FAMILY='").append(columnFamily).append("'").toString();
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
