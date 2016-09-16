package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.QueryUtil;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;

/**
 * Created on 15/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixHelper {

    public static final String STATS_PREFIX = "";
    public static final byte[] STATS_PREFIX_BYTES = Bytes.toBytes(STATS_PREFIX);
    public static final String ANNOTATION_PREFIX = "A_";
    public static final String POPULATION_FREQUENCY_PREFIX = ANNOTATION_PREFIX + "PF_";
    public static final String FUNCTIONAL_SCORE_PREFIX = ANNOTATION_PREFIX + "FS_";
    public static final String STATS_PROTOBUF_SUFIX = "_PB";
    public static final byte[] STATS_PROTOBUF_SUFIX_BYTES = Bytes.toBytes(STATS_PROTOBUF_SUFIX);
    public static final String MAF_SUFIX = "_MAF";
    public static final String MGF_SUFIX = "_MGF";
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
        TYPE("TYPE", PVarchar.INSTANCE),

        SO(ANNOTATION_PREFIX + "SO", PIntegerArray.INSTANCE),
        GENES(ANNOTATION_PREFIX + "GENES", PVarcharArray.INSTANCE),
        BIOTYPE(ANNOTATION_PREFIX + "BIOTYPE", PVarcharArray.INSTANCE),
        TRANSCRIPTS(ANNOTATION_PREFIX + "TRANSCRIPTS", PVarcharArray.INSTANCE),
        TRANSCRIPTION_FLAGS(ANNOTATION_PREFIX + "FLAGS", PVarcharArray.INSTANCE),
        GENE_TRAITS_NAME(ANNOTATION_PREFIX + "GT_NAME", PVarcharArray.INSTANCE),
        GENE_TRAITS_ID(ANNOTATION_PREFIX + "GT_ID", PVarcharArray.INSTANCE),
        HPO(ANNOTATION_PREFIX + "HPO", PVarcharArray.INSTANCE),
        PROTEIN_KEYWORDS(ANNOTATION_PREFIX + "PROT_KW", PVarcharArray.INSTANCE),
        DRUG(ANNOTATION_PREFIX + "DRUG", PVarcharArray.INSTANCE),
        XREFS(ANNOTATION_PREFIX + "XREFS", PVarcharArray.INSTANCE),

        //Protein substitution scores
        POLYPHEN(ANNOTATION_PREFIX + "POLYPHEN", PFloatArray.INSTANCE),
        POLYPHEN_DESC(ANNOTATION_PREFIX + "POLYPHEN_DESC", PVarcharArray.INSTANCE),
        SIFT(ANNOTATION_PREFIX + "SIFT", PFloatArray.INSTANCE),
        SIFT_DESC(ANNOTATION_PREFIX + "SIFT_DESC", PVarcharArray.INSTANCE),

        //Conservation Scores
        PHASTCONS(ANNOTATION_PREFIX + "PHASTCONS", PFloat.INSTANCE),
        PHYLOP(ANNOTATION_PREFIX + "PHYLOP", PFloat.INSTANCE),
        GERP(ANNOTATION_PREFIX + "GERP", PFloat.INSTANCE),

        FULL_ANNOTATION(ANNOTATION_PREFIX + "FULL", PVarchar.INSTANCE);

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

    public Connection newJdbcConnection(Configuration conf) throws SQLException, ClassNotFoundException {
        return QueryUtil.getConnection(conf);
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
            for (Column column : getStatsColumns(studyConfiguration.getStudyId(), cohortId)) {
                String sql = buildAlterViewAddColumn(tableName, column.column(), column.sqlType(), true);
                execute(con, sql);
            }
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
        try (Statement statement = con.createStatement()) {
            statement.execute(sql);
        }
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

    public static List<Column> getStatsColumns(int studyId, int cohortId) {
        return Arrays.asList(getStatsColumn(studyId, cohortId), getMafColumn(studyId, cohortId), getMgfColumn(studyId, cohortId));
    }

    public static Column getStatsColumn(int studyId, int cohortId) {
        return Column.build(STATS_PREFIX + studyId + "_" + cohortId + STATS_PROTOBUF_SUFIX, PVarbinary.INSTANCE);
    }

    public static Column getStudyColumn(int studyId) {
        return Column.build(VariantTableStudyRow.buildColumnKey(studyId, VariantTableStudyRow.HOM_REF), PUnsignedInt.INSTANCE);
    }

    public static Column getMafColumn(int studyId, int cohortId) {
        return Column.build(STATS_PREFIX + studyId + "_" + cohortId + MAF_SUFIX, PFloat.INSTANCE);
    }

    public static Column getMgfColumn(int studyId, int cohortId) {
        return Column.build(STATS_PREFIX + studyId + "_" + cohortId + MGF_SUFIX, PFloat.INSTANCE);
    }

    public static byte[] toBytes(Collection collection, PArrayDataType arrayType) {
        PDataType pDataType = PDataType.arrayBaseType(arrayType);
        Object[] elements = collection.toArray();
        PhoenixArray phoenixArray = new PhoenixArray(pDataType, elements);
        return arrayType.toBytes(phoenixArray);
    }

}
