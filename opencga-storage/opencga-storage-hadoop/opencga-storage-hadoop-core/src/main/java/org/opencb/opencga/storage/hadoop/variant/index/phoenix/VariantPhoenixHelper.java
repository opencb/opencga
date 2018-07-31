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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.*;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper.Column;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.ANNOT_CONSERVATION;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.ANNOT_FUNCTIONAL_SCORE;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;

/**
 * Created on 15/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixHelper {

    // TODO: Make default varants table type configurable
    public static final PTableType DEFAULT_TABLE_TYPE = PTableType.VIEW;

    public static final String STATS_PREFIX = "";
    public static final byte[] STATS_PREFIX_BYTES = Bytes.toBytes(STATS_PREFIX);
    public static final String ANNOTATION_PREFIX = "A_";
    public static final String POPULATION_FREQUENCY_PREFIX = ANNOTATION_PREFIX + "PF_";
    public static final String FUNCTIONAL_SCORE_PREFIX = ANNOTATION_PREFIX + "FS_";
    public static final String STATS_PROTOBUF_SUFIX = "_PB";
    public static final String SAMPLE_DATA_SUFIX = "_S";
    public static final byte[] SAMPLE_DATA_SUFIX_BYTES = Bytes.toBytes(SAMPLE_DATA_SUFIX);
    public static final String FILE_SUFIX = "_F";
    public static final byte[] FILE_SUFIX_BYTES = Bytes.toBytes(FILE_SUFIX);
    public static final String STUDY_SUFIX = "_ST";
    public static final byte[] STUDY_SUFIX_BYTES = Bytes.toBytes(STUDY_SUFIX);
    public static final byte[] STATS_PROTOBUF_SUFIX_BYTES = Bytes.toBytes(STATS_PROTOBUF_SUFIX);
    public static final String MAF_SUFIX = "_MAF";
    public static final String MGF_SUFIX = "_MGF";
    public static final char COLUMN_KEY_SEPARATOR = '_';
    public static final String COLUMN_KEY_SEPARATOR_STR = String.valueOf(COLUMN_KEY_SEPARATOR);
    public static final String RELEASE_PREFIX = "R_";
    public static final byte[] RELEASE_PREFIX_BYTES = Bytes.toBytes(RELEASE_PREFIX);
    public static final String HOM_REF = "0/0";
    public static final byte[] HOM_REF_BYTES = Bytes.toBytes(HOM_REF);
    private static final String STUDY_POP_FREQ_SEPARATOR = "_";
    public static final List<Column> PRIMARY_KEY = Collections.unmodifiableList(Arrays.asList(CHROMOSOME, POSITION, REFERENCE, ALTERNATE));
    public static final String FILL_MISSING_SUFIX = "_FM";
    public static final byte[] FILL_MISSING_SUFIX_BYTES = Bytes.toBytes(FILL_MISSING_SUFIX);

    protected static Logger logger = LoggerFactory.getLogger(VariantPhoenixHelper.class);

    private final PhoenixHelper phoenixHelper;
    private final GenomeHelper genomeHelper;


    public enum VariantColumn implements Column {
        CHROMOSOME("CHROMOSOME", PVarchar.INSTANCE),
        POSITION("POSITION", PUnsignedInt.INSTANCE),
        REFERENCE("REFERENCE", PVarchar.INSTANCE),
        ALTERNATE("ALTERNATE", PVarchar.INSTANCE),

        TYPE("TYPE", PVarchar.INSTANCE),

        SO(ANNOTATION_PREFIX + "SO", PIntegerArray.INSTANCE),
        GENES(ANNOTATION_PREFIX + "GENES", PVarcharArray.INSTANCE),
        GENE_SO(ANNOTATION_PREFIX + "GENE_SO", PVarcharArray.INSTANCE),
        BIOTYPE(ANNOTATION_PREFIX + "BIOTYPE", PVarcharArray.INSTANCE),
        TRANSCRIPTS(ANNOTATION_PREFIX + "TRANSCRIPTS", PVarcharArray.INSTANCE),
        TRANSCRIPTION_FLAGS(ANNOTATION_PREFIX + "FLAGS", PVarcharArray.INSTANCE),
        GENE_TRAITS_NAME(ANNOTATION_PREFIX + "GT_NAME", PVarcharArray.INSTANCE),
        GENE_TRAITS_ID(ANNOTATION_PREFIX + "GT_ID", PVarcharArray.INSTANCE),
        CLINICAL_SIGNIFICANCE(ANNOTATION_PREFIX + "CLI_SIG", PVarcharArray.INSTANCE),
//        HPO(ANNOTATION_PREFIX + "HPO", PVarcharArray.INSTANCE),
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

        //Functional Scores
        CADD_SCALED(FUNCTIONAL_SCORE_PREFIX + "CADD_SC", PFloat.INSTANCE),
        CADD_RAW(FUNCTIONAL_SCORE_PREFIX + "CADD_R", PFloat.INSTANCE),

        FULL_ANNOTATION(ANNOTATION_PREFIX + "FULL", PVarchar.INSTANCE);

        private final String columnName;
        private final byte[] columnNameBytes;
        private PDataType pDataType;
        private final String sqlTypeName;
        private final boolean nullable;

        private static Map<String, Column> columns = null;

        VariantColumn(String columnName, PDataType pDataType) {
            this.columnName = columnName;
            this.pDataType = pDataType;
            this.sqlTypeName = pDataType.getSqlTypeName();
            columnNameBytes = Bytes.toBytes(columnName);
            nullable = false;
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
        public boolean nullable() {
            return nullable;
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

    private static final Map<String, String> MAPPING_POPULATION_SUDIES;
    private static final List<Column> HUMAN_POPULATION_FREQUENCIES_COLUMNS;

    static {
        HashMap<String, String> mappingPopulationStudies = new HashMap<>(2);
        mappingPopulationStudies.put("1000GENOMES_PHASE_3", "1KG_PHASE3");
        mappingPopulationStudies.put("ESP_6500", "ESP6500");
        MAPPING_POPULATION_SUDIES = Collections.unmodifiableMap(mappingPopulationStudies);

        HUMAN_POPULATION_FREQUENCIES_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            getPopulationFrequencyColumn("1kG_phase3", "ALL"),

            getPopulationFrequencyColumn("1kG_phase3", "AFR"),
            getPopulationFrequencyColumn("1kG_phase3", "AMR"),
            getPopulationFrequencyColumn("1kG_phase3", "EAS"),
            getPopulationFrequencyColumn("1kG_phase3", "EUR"),
            getPopulationFrequencyColumn("1kG_phase3", "SAS"),

            getPopulationFrequencyColumn("1kG_phase3", "ACB"),
            getPopulationFrequencyColumn("1kG_phase3", "ASW"),
            getPopulationFrequencyColumn("1kG_phase3", "BEB"),
            getPopulationFrequencyColumn("1kG_phase3", "CDX"),
            getPopulationFrequencyColumn("1kG_phase3", "CEU"),
            getPopulationFrequencyColumn("1kG_phase3", "CHB"),
            getPopulationFrequencyColumn("1kG_phase3", "CHD"),
            getPopulationFrequencyColumn("1kG_phase3", "CHS"),
            getPopulationFrequencyColumn("1kG_phase3", "CLM"),
            getPopulationFrequencyColumn("1kG_phase3", "ESN"),
            getPopulationFrequencyColumn("1kG_phase3", "FIN"),
            getPopulationFrequencyColumn("1kG_phase3", "GBR"),
            getPopulationFrequencyColumn("1kG_phase3", "GIH"),
            getPopulationFrequencyColumn("1kG_phase3", "GWD"),
            getPopulationFrequencyColumn("1kG_phase3", "IBS"),
            getPopulationFrequencyColumn("1kG_phase3", "ITU"),
            getPopulationFrequencyColumn("1kG_phase3", "JPT"),
            getPopulationFrequencyColumn("1kG_phase3", "KHV"),
            getPopulationFrequencyColumn("1kG_phase3", "LWK"),
            getPopulationFrequencyColumn("1kG_phase3", "MSL"),
            getPopulationFrequencyColumn("1kG_phase3", "MXL"),
            getPopulationFrequencyColumn("1kG_phase3", "PEL"),
            getPopulationFrequencyColumn("1kG_phase3", "PJL"),
            getPopulationFrequencyColumn("1kG_phase3", "PUR"),
            getPopulationFrequencyColumn("1kG_phase3", "STU"),
            getPopulationFrequencyColumn("1kG_phase3", "TSI"),
            getPopulationFrequencyColumn("1kG_phase3", "YRI"),

            getPopulationFrequencyColumn("ESP6500", "ALL"),
            getPopulationFrequencyColumn("ESP6500", "EA"),
            getPopulationFrequencyColumn("ESP6500", "AA"),

            getPopulationFrequencyColumn("EXAC", "ALL"),
            getPopulationFrequencyColumn("EXAC", "AFR"),
            getPopulationFrequencyColumn("EXAC", "AMR"),
            getPopulationFrequencyColumn("EXAC", "EAS"),
            getPopulationFrequencyColumn("EXAC", "FIN"),
            getPopulationFrequencyColumn("EXAC", "NFE"),
            getPopulationFrequencyColumn("EXAC", "OTH"),
            getPopulationFrequencyColumn("EXAC", "SAS"),

            getPopulationFrequencyColumn("GONL", "ALL"),

            getPopulationFrequencyColumn("UK10K_ALSPAC", "ALL"),
            getPopulationFrequencyColumn("UK10K_TWINSUK", "ALL")
        ));
    }


    public static List<Column> getHumanPopulationFrequenciesColumns() {
        return HUMAN_POPULATION_FREQUENCIES_COLUMNS;
    }

    public VariantPhoenixHelper(GenomeHelper genomeHelper) {
        this.genomeHelper = genomeHelper;
        phoenixHelper = new PhoenixHelper(genomeHelper.getConf());
    }

    public Connection newJdbcConnection() throws SQLException, ClassNotFoundException {
        return phoenixHelper.newJdbcConnection(genomeHelper.getConf());
    }

    public Connection newJdbcConnection(Configuration conf) throws SQLException, ClassNotFoundException {
        return phoenixHelper.newJdbcConnection(conf);
    }

    public PhoenixHelper getPhoenixHelper() {
        return phoenixHelper;
    }

    public void updateAnnotationColumns(Connection con, String variantsTableName) throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        List<Column> annotColumns = Arrays.asList(VariantColumn.values());
        phoenixHelper.addMissingColumns(con, variantsTableName, annotColumns, true, DEFAULT_TABLE_TYPE);
    }

    public void updateStatsColumns(Connection con, String variantsTableName, StudyConfiguration studyConfiguration) throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        List<Column> columns = new ArrayList<>();
        for (Integer cohortId : studyConfiguration.getCalculatedStats()) {
            for (Column column : getStatsColumns(studyConfiguration.getStudyId(), cohortId)) {
                columns.add(column);
            }
        }
        phoenixHelper.addMissingColumns(con, variantsTableName, columns, true, DEFAULT_TABLE_TYPE);
    }

    public void registerNewStudy(Connection con, String variantsTableName, Integer studyId) throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        createTableIfNeeded(con, variantsTableName);
        List<Column> columns = Arrays.asList(getStudyColumn(studyId), getFillMissingColumn(studyId));
        addMissingColumns(con, variantsTableName, columns, true);
        con.commit();
    }

    public void registerNewFiles(Connection con, String variantsTableName, Integer studyId, Collection<Integer> fileIds,
                                 Collection<Integer> sampleIds) throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        createTableIfNeeded(con, variantsTableName);
        List<Column> columns = new ArrayList<>(fileIds.size() + sampleIds.size() + 1);
        for (Integer fileId : fileIds) {
            columns.add(getFileColumn(studyId, fileId));
        }
        for (Integer sampleId : sampleIds) {
            columns.add(getSampleColumn(studyId, sampleId));
        }
        phoenixHelper.addMissingColumns(con, variantsTableName, columns, true, DEFAULT_TABLE_TYPE);
        con.commit();
    }

    public void registerRelease(Connection con, String table, int release) throws SQLException {
        List<Column> columns = new ArrayList<>(release);
        for (int i = 1; i <= release; i++) {
            columns.add(getReleaseColumn(i));
        }
        phoenixHelper.addMissingColumns(con, table, columns, true, DEFAULT_TABLE_TYPE);
        con.commit();
    }

    public void dropFiles(Connection con, String variantsTableName, Integer studyId, Collection<Integer> fileIds,
                          Collection<Integer> sampleIds) throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        List<CharSequence> columns = new ArrayList<>(fileIds.size() + sampleIds.size());
        for (Integer fileId : fileIds) {
            columns.add(buildFileColumnKey(studyId, fileId, new StringBuilder()));
        }
        for (Integer sampleId : sampleIds) {
            columns.add(buildSampleColumnKey(studyId, sampleId, new StringBuilder()));
        }
        phoenixHelper.dropColumns(con, variantsTableName, columns, DEFAULT_TABLE_TYPE);
        con.commit();
    }

    public void createSchemaIfNeeded(Connection con, String schema) throws SQLException {
        String sql = "CREATE SCHEMA IF NOT EXISTS \"" + schema + "\"";
        logger.debug(sql);
        try {
            phoenixHelper.execute(con, sql);
        } catch (SQLException e) {
            if (e.getCause() != null && e.getCause() instanceof NamespaceExistException) {
                logger.debug("Namespace already exists", e);
            } else {
                throw e;
            }
        }
    }

    public void createTableIfNeeded(Connection con, String table) throws SQLException {
        if (!phoenixHelper.tableExists(con, table)) {
            String sql = buildCreate(table);
            logger.info(sql);
            try {
                phoenixHelper.execute(con, sql);
            } catch (Exception e) {
                if (!phoenixHelper.tableExists(con, table)) {
                    throw e;
                } else {
                    logger.info(DEFAULT_TABLE_TYPE + " {} already exists", table);
                    logger.debug(DEFAULT_TABLE_TYPE + " " + table + " already exists. Hide exception", e);
                }
            }
        } else {
            logger.debug(DEFAULT_TABLE_TYPE + " {} already exists", table);
        }
    }

    public void addMissingColumns(Connection connection, String variantsTableName, List<Column> newColumns, boolean oneCall)
            throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        phoenixHelper.addMissingColumns(connection, variantsTableName, newColumns, oneCall, DEFAULT_TABLE_TYPE);
    }

    private String buildCreate(String variantsTableName) {
        return buildCreate(variantsTableName, Bytes.toString(genomeHelper.getColumnFamily()), DEFAULT_TABLE_TYPE);
    }

    private String buildCreate(String variantsTableName, String columnFamily, PTableType tableType) {
        StringBuilder sb = new StringBuilder().append("CREATE ").append(tableType).append(" IF NOT EXISTS ")
                .append(phoenixHelper.getEscapedFullTableName(tableType, variantsTableName)).append(' ').append('(');
        for (VariantColumn variantColumn : VariantColumn.values()) {
            switch (variantColumn) {
                case CHROMOSOME:
                case POSITION:
                    sb.append(' ').append(variantColumn).append(' ').append(variantColumn.sqlType()).append(" NOT NULL , ");
                    break;
                default:
                    sb.append(' ').append(variantColumn).append(' ').append(variantColumn.sqlType()).append(" , ");
                    break;
            }
        }

//        for (Column column : VariantPhoenixHelper.HUMAN_POPULATION_FREQUENCIES_COLUMNS) {
//            sb.append(" \"").append(column).append("\" ").append(column.sqlType()).append(" , ");
//        }

        sb.append(" CONSTRAINT PK PRIMARY KEY (");
        for (Iterator<Column> iterator = PRIMARY_KEY.iterator(); iterator.hasNext();) {
            Column column = iterator.next();
            sb.append(column);
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.append(") )").toString();
    }

    public void createVariantIndexes(Connection con, String variantsTableName) throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        List<PhoenixHelper.Index> indices = getIndices(variantsTableName);
        phoenixHelper.createIndexes(con, DEFAULT_TABLE_TYPE, variantsTableName, indices, false);
    }

    public static List<PhoenixHelper.Index> getPopFreqIndices(String variantsTableName) {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        return Arrays.asList(getPopFreqIndex(variantsTableName, "1kG_phase3", "ALL"), getPopFreqIndex(variantsTableName, "EXAC", "ALL"));
    }

    public static PhoenixHelper.Index getPopFreqIndex(String variantsTableName, String study, String population) {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        TableName table = TableName.valueOf(variantsTableName);
        Column column = getPopulationFrequencyColumn(study, population);
        List<Column> defaultInclude = Arrays.asList(GENES, SO);
        return new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(
                "\"" + column.column() + "\"[2]",
                "\"" + column.column() + "\"[1]"), defaultInclude);
    }

    private static List<PhoenixHelper.Index> getIndices(String variantsTableName) {
        TableName table = TableName.valueOf(variantsTableName);
        List<Column> defaultInclude = Arrays.asList(GENES, SO);
        return Arrays.asList(
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(PHASTCONS), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(PHYLOP), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(GERP), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(CADD_RAW), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(CADD_SCALED), defaultInclude),
                // Index the min value
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList("\"" + POLYPHEN + "\"[1]"), defaultInclude),
                // Index the max value
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList("\"" + SIFT + "\"[2]"), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(TYPE), defaultInclude)

//                new PhoenixHelper.Index("POLYPHEN_IDX", PTable.IndexType.LOCAL,
//                        Arrays.asList(CHROMOSOME.column(), POSITION.column(), REFERENCE.column(), ALTERNATE.column(), POLYPHEN.column()),
//                        Arrays.asList(TYPE.column())),
//                new PhoenixHelper.Index("SIFT_IDX", PTable.IndexType.LOCAL,
//                        Arrays.asList(CHROMOSOME.column(), POSITION.column(), REFERENCE.column(), ALTERNATE.column(), SIFT.column()),
//                        Arrays.asList(TYPE.column()))
        );
    }

    public static Column getFunctionalScoreColumn(String source) {
        return getFunctionalScoreColumn(source, true, source);
    }

    public static Column getFunctionalScoreColumn(String source, String rawValue) {
        return getFunctionalScoreColumn(source, true, rawValue);
    }

    public static Column getFunctionalScoreColumn(String source, boolean throwException, String rawValue) {
        switch (source.toUpperCase()) {
            case "CADD_RAW":
                return CADD_RAW;
            case "CADD_SCALED":
                return CADD_SCALED;
            default:
                if (throwException) {
//                    throw VariantQueryException.malformedParam(ANNOT_FUNCTIONAL_SCORE, rawValue, "Unknown functional score.");
                    throw VariantQueryException.malformedParam(ANNOT_FUNCTIONAL_SCORE, rawValue);
                } else {
                    logger.warn("Unknown Conservation source {}", source);
                }
        }
        return Column.build(FUNCTIONAL_SCORE_PREFIX + source.toUpperCase(), PFloat.INSTANCE);
    }

    public static Column getPopulationFrequencyColumn(String study, String population) {
        study = study.toUpperCase();
        for (Map.Entry<String, String> entry : MAPPING_POPULATION_SUDIES.entrySet()) {
            study = study.replace(entry.getKey(), entry.getValue());
        }
        return Column.build(POPULATION_FREQUENCY_PREFIX + study + STUDY_POP_FREQ_SEPARATOR + population.toUpperCase(),
                PFloatArray.INSTANCE);
    }

    public static Column getPopulationFrequencyColumn(String studyPopulation) {
        studyPopulation = studyPopulation.toUpperCase();
        for (Map.Entry<String, String> entry : MAPPING_POPULATION_SUDIES.entrySet()) {
            studyPopulation = studyPopulation.replace(entry.getKey(), entry.getValue());
        }
        String studyPopFreq = studyPopulation.replace(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR, STUDY_POP_FREQ_SEPARATOR);
        return Column.build(POPULATION_FREQUENCY_PREFIX + studyPopFreq, PFloatArray.INSTANCE);
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
                    throw VariantQueryException.malformedParam(ANNOT_CONSERVATION, rawValue);
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
        return Column.build(STATS_PREFIX + studyId + COLUMN_KEY_SEPARATOR + cohortId + STATS_PROTOBUF_SUFIX, PVarbinary.INSTANCE);
    }

    public static Column getStudyColumn(int studyId) {
        return Column.build(String.valueOf(studyId) + STUDY_SUFIX, PUnsignedInt.INSTANCE);
    }

    public static Integer extractStudyId(String columnKey, boolean failOnMissing) {
        int endIndex = columnKey.indexOf(COLUMN_KEY_SEPARATOR);
        if (endIndex > 0) {
            String study = columnKey.substring(0, endIndex);
            if (StringUtils.isNotBlank(columnKey)
                    && Character.isDigit(columnKey.charAt(0))
                    && StringUtils.isNumeric(study)) {
                return Integer.parseInt(study);
            }
        }
        if (failOnMissing) {
            throw new IllegalStateException("Integer expected for study ID from " + columnKey);
        } else {
            return null;
        }
    }

    public static Column getMafColumn(int studyId, int cohortId) {
        return Column.build(STATS_PREFIX + studyId + "_" + cohortId + MAF_SUFIX, PFloat.INSTANCE);
    }

    public static Column getMgfColumn(int studyId, int cohortId) {
        return Column.build(STATS_PREFIX + studyId + "_" + cohortId + MGF_SUFIX, PFloat.INSTANCE);
    }

    public static byte[] buildSampleColumnKey(int studyId, int sampleId) {
        return Bytes.toBytes(buildSampleColumnKey(studyId, sampleId, new StringBuilder()).toString());
    }

    public static StringBuilder buildSampleColumnKey(int studyId, int sampleId, StringBuilder stringBuilder) {
        return stringBuilder.append(studyId).append(COLUMN_KEY_SEPARATOR).append(sampleId).append(SAMPLE_DATA_SUFIX);
    }

    public static Column getSampleColumn(int studyId, int sampleId) {
        return Column.build(buildSampleColumnKey(studyId, sampleId, new StringBuilder()).toString(), PVarcharArray.INSTANCE);
    }

    public static byte[] buildFileColumnKey(int studyId, int fileId) {
        return Bytes.toBytes(buildFileColumnKey(studyId, fileId, new StringBuilder()).toString());
    }

    public static StringBuilder buildFileColumnKey(int studyId, int fileId, StringBuilder stringBuilder) {
        return stringBuilder.append(studyId).append(COLUMN_KEY_SEPARATOR).append(fileId).append(FILE_SUFIX);
    }

    public static Column getFileColumn(int studyId, int sampleId) {
        return Column.build(buildFileColumnKey(studyId, sampleId, new StringBuilder()).toString(), PVarcharArray.INSTANCE);
    }

    public static byte[] buildReleaseColumnKey(int release) {
        return Bytes.toBytes(buildReleaseColumnKey(release, new StringBuilder()).toString());
    }

    public static StringBuilder buildReleaseColumnKey(int release, StringBuilder stringBuilder) {
        return stringBuilder.append(RELEASE_PREFIX).append(release);
    }

    public static Column getReleaseColumn(int release) {
        return Column.build(buildReleaseColumnKey(release, new StringBuilder()).toString(), PBoolean.INSTANCE);
    }

    public static Column getFillMissingColumn(int studyId) {
        return Column.build("_" + studyId + FILL_MISSING_SUFIX, PInteger.INSTANCE);
    }

    public static String getEscapedFullTableName(String fullTableName, Configuration conf) {
        return PhoenixHelper.getEscapedFullTableName(DEFAULT_TABLE_TYPE, fullTableName, conf);
    }

    public static String getAnnotationSnapshotColumn(int id) {
        if (id <= 0) {
            throw new IllegalArgumentException("Wrong annotation snapshot column id. Must be greater than 0. Found: " + id);
        }
        return "A_" + id;
    }
}
