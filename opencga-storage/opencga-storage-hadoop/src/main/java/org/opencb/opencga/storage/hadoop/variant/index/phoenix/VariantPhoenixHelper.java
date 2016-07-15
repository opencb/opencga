package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.QueryUtil;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;

/**
 * Created on 15/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixHelper {

    public static final String STATS_PREFIX = "S_";
    public static final byte[] STATS_PREFIX_BYTES = Bytes.toBytes(STATS_PREFIX);
    public static final String POPULATION_FREQUENCY_PREFIX = "A_PF_";
    public static final String FUNCTIONAL_SCORE_PREFIX = "A_FS_";
    public static final String PROTOBUF_SUFIX = "_PB";
    public static final byte[] PROTOBUF_SUFIX_BYTES = Bytes.toBytes(PROTOBUF_SUFIX);
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

        SO("A_SO", PIntegerArray.INSTANCE),
        GENES("A_GENES", PVarcharArray.INSTANCE),
        BIOTYPE("A_BIOTYPE", PVarcharArray.INSTANCE),
        TRANSCRIPTS("A_TRANSCRIPTS", PVarcharArray.INSTANCE),
        TRANSCRIPTION_FLAGS("A_FLAGS", PVarcharArray.INSTANCE),
        GENE_TRAITS_NAME("A_GT_NAME", PVarcharArray.INSTANCE),
        GENE_TRAITS_ID("A_GT_ID", PVarcharArray.INSTANCE),
        PROTEIN_KEYWORDS("A_PROT_KW", PVarcharArray.INSTANCE),
        DRUG("A_DRUG", PVarcharArray.INSTANCE),
        XREFS("A_XREFS", PVarcharArray.INSTANCE),

        //Protein substitution scores
        POLYPHEN("A_POLYPHEN", PFloatArray.INSTANCE),
        POLYPHEN_DESC("A_POLYPHEN_DESC", PVarcharArray.INSTANCE),
        SIFT("A_SIFT", PFloatArray.INSTANCE),
        SIFT_DESC("A_SIFT_DESC", PVarcharArray.INSTANCE),

        //Conservation Scores
        PHASTCONS("A_PHASTCONS", PFloat.INSTANCE),
        PHYLOP("A_PHYLOP", PFloat.INSTANCE),
        GERP("A_GERP", PFloat.INSTANCE),

        FULL_ANNOTATION("A_FULL", PVarchar.INSTANCE);

        private final String columnName;
        private final byte[] columnNameBytes;
        private PDataType pDataType;
        private final String sqlTypeName;

        private static Map<String, Column> columns = null;

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

        public static Column getColumn(String columnName) {
            if (columns == null) {
                Map<String, Column> map = new HashMap<>();
                for (VariantColumn column : VariantColumn.values()) {
                    map.put(column.column(), column);
                }
                columns = map;
            }
            return columns.get(columnName);
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

    public void updateStatsFields(Connection con, String tableName, StudyConfiguration studyConfiguration) throws SQLException {
        for (Integer cohortId : studyConfiguration.getCohortIds().values()) {
            Column column = getMafColumn(studyConfiguration.getStudyId(), cohortId);
            String sql = buildAlterViewAddColumn(tableName, column.column(), column.sqlType(), true);
            execute(con, sql);
            column = getStatsColumn(studyConfiguration.getStudyId(), cohortId);
            sql = buildAlterViewAddColumn(tableName, column.column(), column.sqlType(), true);
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

        return sb.append(" ")
                .append("CONSTRAINT PK PRIMARY KEY (")
                    .append(CHROMOSOME).append(", ")
                    .append(POSITION).append(", ")
                    .append(REFERENCE).append(", ")
                    .append(ALTERNATE).append(") ").append(") ")
                .append("DEFAULT_COLUMN_FAMILY='").append(columnFamily).append("'").toString();
    }

    public String buildAlterViewAddColumn(String tableName, String column, String type) {
        return buildAlterViewAddColumn(tableName, column, type, true);
    }

    public String buildAlterViewAddColumn(String tableName, String column, String type, boolean ifNotExists) {
        return "ALTER VIEW \"" + tableName + "\" ADD " + (ifNotExists ? "IF NOT EXISTS " : "") + "\"" + column + "\" " + type;
    }

    public static Column getFunctionalScoreColumn(String source) {
        return Column.build(FUNCTIONAL_SCORE_PREFIX + source.toUpperCase(), PFloat.INSTANCE);
    }

    public static Column getPopulationFrequencyColumn(String study, String population) {
        return Column.build(POPULATION_FREQUENCY_PREFIX + study.toUpperCase() + ":" + population.toUpperCase(), PFloatArray.INSTANCE);
    }

    public static Column getPopulationFrequencyColumn(String studyPopulation) {
        return Column.build(POPULATION_FREQUENCY_PREFIX + studyPopulation.toUpperCase(), PFloatArray.INSTANCE);
    }

    public static Column getConservationScoreColumn(String source)
            throws VariantQueryException {
        return getConservationScoreColumn(source, source, true);
    }

    public static Column getConservationScoreColumn(String source, String rawValue, boolean throwException)
            throws VariantQueryException {
        source = source.toUpperCase();
        switch (source) {
            case "PHASTCONS":
                return PHASTCONS;
            case "PHYLOP":
                return PHYLOP;
            case "GERP":
                return GERP;
            default:
                if (throwException) {
                    throw VariantQueryException.malformedParam(ANNOT_CONSERVATION, rawValue, "Unknown conservation value.");
                } else {
                    logger.warn("Unknown Conservation source {}", rawValue);
                }
                return null;
        }
    }

    public static Column getMafColumn(int studyId, int cohortId) {
        return Column.build(STATS_PREFIX + studyId + "_" + cohortId + "_MAF", PFloat.INSTANCE);
    }

    public static Column getStatsColumn(int studyId, int cohortId) {
        return Column.build(STATS_PREFIX + studyId + "_" + cohortId + PROTOBUF_SUFIX, PVarbinary.INSTANCE);
    }

    public static byte[] toBytes(Collection collection, PArrayDataType arrayType) {
        PDataType pDataType = PDataType.arrayBaseType(arrayType);
        Object[] elements = collection.toArray();
        PhoenixArray phoenixArray = new PhoenixArray(pDataType, elements);
        return arrayType.toBytes(phoenixArray);
    }

}
