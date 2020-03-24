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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper.Column;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.ANNOT_CONSERVATION;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.ANNOT_FUNCTIONAL_SCORE;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.VariantColumn.*;

/**
 * Created on 15/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixHelper {

    // TODO: Make default varants table type configurable
    public static final PTableType DEFAULT_TABLE_TYPE = PTableType.VIEW;

    public static final String ANNOTATION_PREFIX = "A_";
    public static final String POPULATION_FREQUENCY_PREFIX = ANNOTATION_PREFIX + "PF_";
    public static final String FUNCTIONAL_SCORE_PREFIX = ANNOTATION_PREFIX + "FS_";
    public static final String SAMPLE_DATA_SUFIX = "_S";
    public static final byte[] SAMPLE_DATA_SUFIX_BYTES = Bytes.toBytes(SAMPLE_DATA_SUFIX);
    public static final String FILE_SUFIX = "_F";
    public static final byte[] FILE_SUFIX_BYTES = Bytes.toBytes(FILE_SUFIX);
    public static final String STUDY_SUFIX = "_ST";
    public static final byte[] STUDY_SUFIX_BYTES = Bytes.toBytes(STUDY_SUFIX);

    public static final String COHORT_STATS_PROTOBUF_SUFFIX = "_PB";
    public static final byte[] COHORT_STATS_PROTOBUF_SUFFIX_BYTES = Bytes.toBytes(COHORT_STATS_PROTOBUF_SUFFIX);
    public static final String COHORT_STATS_FREQ_SUFFIX = "_CF";
    public static final String COHORT_STATS_MAF_SUFFIX = "_MAF";
    public static final String COHORT_STATS_MGF_SUFFIX = "_MGF";
    public static final String COHORT_STATS_PASS_FREQ_SUFFIX = "_PSF";

    public static final char COLUMN_KEY_SEPARATOR = '_';
    public static final String COLUMN_KEY_SEPARATOR_STR = String.valueOf(COLUMN_KEY_SEPARATOR);
    public static final String RELEASE_PREFIX = "R_";
    public static final byte[] RELEASE_PREFIX_BYTES = Bytes.toBytes(RELEASE_PREFIX);

    private static final String STUDY_POP_FREQ_SEPARATOR = "_";
    public static final List<Column> PRIMARY_KEY = Collections.unmodifiableList(Arrays.asList(
            CHROMOSOME,
            POSITION,
            REFERENCE,
            ALTERNATE
    ));

    public static final String FILL_MISSING_SUFIX = "_FM";
    public static final byte[] FILL_MISSING_SUFIX_BYTES = Bytes.toBytes(FILL_MISSING_SUFIX);
    public static final String VARIANT_SCORE_SUFIX = "_VS";
    public static final byte[] VARIANT_SCORE_SUFIX_BYTES = Bytes.toBytes(VARIANT_SCORE_SUFIX);

    protected static Logger logger = LoggerFactory.getLogger(VariantPhoenixHelper.class);

    private final PhoenixHelper phoenixHelper;
    private final byte[] columnFamily;
    private final Configuration conf;


    public enum VariantColumn implements Column {
        CHROMOSOME("CHROMOSOME", PVarchar.INSTANCE),
        POSITION("POSITION", PUnsignedInt.INSTANCE),
        REFERENCE("REFERENCE", PVarchar.INSTANCE),
        ALTERNATE("ALTERNATE", PVarchar.INSTANCE),

        CI_START_L("CI_START_L", PUnsignedInt.INSTANCE),
        CI_START_R("CI_START_R", PUnsignedInt.INSTANCE),
        CI_END_L("CI_END_L", PUnsignedInt.INSTANCE),
        CI_END_R("CI_END_R", PUnsignedInt.INSTANCE),

        TYPE("TYPE", PVarchar.INSTANCE),

        ANNOTATION_ID(ANNOTATION_PREFIX + "ID", PInteger.INSTANCE),

        SO(ANNOTATION_PREFIX + "SO", PIntegerArray.INSTANCE),
        GENES(ANNOTATION_PREFIX + "GENES", PVarcharArray.INSTANCE),
        GENE_SO(ANNOTATION_PREFIX + "GENE_SO", PVarcharArray.INSTANCE),
        BIOTYPE_SO(ANNOTATION_PREFIX + "BT_SO", PVarcharArray.INSTANCE),
        GENE_BIOTYPE_SO(ANNOTATION_PREFIX + "GENE_BT_SO", PVarcharArray.INSTANCE),
        GENE_BIOTYPE(ANNOTATION_PREFIX + "GENE_BT", PVarcharArray.INSTANCE),
        GENE_SO_FLAG(ANNOTATION_PREFIX + "GENE_SO_FLAG", PVarcharArray.INSTANCE),
        SO_FLAG(ANNOTATION_PREFIX + "SO_FLAG", PVarcharArray.INSTANCE),

        BIOTYPE(ANNOTATION_PREFIX + "BIOTYPE", PVarcharArray.INSTANCE),
        TRANSCRIPTS(ANNOTATION_PREFIX + "TRANSCRIPTS", PVarcharArray.INSTANCE),
        TRANSCRIPT_FLAGS(ANNOTATION_PREFIX + "FLAGS", PVarcharArray.INSTANCE),
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

        FULL_ANNOTATION(ANNOTATION_PREFIX + "FULL", PVarchar.INSTANCE),

        INDEX_NOT_SYNC("_IDX_N", PBoolean.INSTANCE),
        INDEX_UNKNOWN("_IDX_U", PBoolean.INSTANCE),
        INDEX_STUDIES("_IDX_ST_", PIntegerArray.INSTANCE);

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
    public static final Set<Column> DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS;

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

            getPopulationFrequencyColumn("UK10K", "ALL"),
            getPopulationFrequencyColumn("UK10K", "ALSPAC"),
            getPopulationFrequencyColumn("UK10K", "TWINSUK"),
            getPopulationFrequencyColumn("UK10K", "TWINSUK_NODUP"),
//            getPopulationFrequencyColumn("UK10K_ALSPAC", "ALL"),
//            getPopulationFrequencyColumn("UK10K_TWINSUK", "ALL"),

            getPopulationFrequencyColumn("GNOMAD_GENOMES", "ALL"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "AFR"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "AMR"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "ASJ"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "EAS"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "FIN"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "NFE"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "OTH"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "MALE"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "FEMALE"),

            getPopulationFrequencyColumn("GNOMAD_EXOMES", "ALL"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "AFR"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "AMR"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "ASJ"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "EAS"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "FIN"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "NFE"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "OTH"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "MALE"),
            getPopulationFrequencyColumn("GNOMAD_EXOMES", "FEMALE")
        ));

        DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            getPopulationFrequencyColumn("1kG_phase3", "ALL"),
            getPopulationFrequencyColumn("1kG_phase3", "AFR"),
            getPopulationFrequencyColumn("1kG_phase3", "AMR"),
            getPopulationFrequencyColumn("1kG_phase3", "EAS"),
            getPopulationFrequencyColumn("1kG_phase3", "EUR"),
            getPopulationFrequencyColumn("1kG_phase3", "SAS"),

            getPopulationFrequencyColumn("GNOMAD_GENOMES", "ALL"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "AFR"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "AMR"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "ASJ"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "EAS"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "FIN"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "NFE"),
            getPopulationFrequencyColumn("GNOMAD_GENOMES", "OTH"),
//            getPopulationFrequencyColumn("GNOMAD_GENOMES", "MALE"),
//            getPopulationFrequencyColumn("GNOMAD_GENOMES", "FEMALE"),

            getPopulationFrequencyColumn("GNOMAD_EXOMES", "ALL"),
            getPopulationFrequencyColumn("ESP6500", "ALL"),
            getPopulationFrequencyColumn("EXAC", "ALL"),
            getPopulationFrequencyColumn("GONL", "ALL"),
            getPopulationFrequencyColumn("UK10K", "ALL")

        )));
    }


    public static List<Column> getHumanPopulationFrequenciesColumns() {
        return HUMAN_POPULATION_FREQUENCIES_COLUMNS;
    }

    public VariantPhoenixHelper(GenomeHelper genomeHelper) {
        this(GenomeHelper.COLUMN_FAMILY_BYTES, genomeHelper.getConf());
    }

    public VariantPhoenixHelper(byte[] columnFamily, Configuration conf) {
        this.columnFamily = columnFamily;
        this.conf = conf;
        phoenixHelper = new PhoenixHelper(conf);
    }

    public Connection newJdbcConnection() throws SQLException, ClassNotFoundException {
        return phoenixHelper.newJdbcConnection(conf);
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

    public void updateStatsColumns(Connection con, String variantsTableName, int studyId, List<Integer> cohortIds) throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        List<Column> columns = new ArrayList<>();
        for (Integer cohortId : cohortIds) {
            columns.addAll(getStatsColumns(studyId, cohortId));
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

    public void dropScore(Connection con, String variantsTableName, Integer studyId, Collection<Integer> scores) throws SQLException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        List<CharSequence> columns = new ArrayList<>(scores.size());
        for (Integer score : scores) {
            columns.add(getVariantScoreColumn(studyId, score).column());
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
        return buildCreate(variantsTableName, Bytes.toString(columnFamily), DEFAULT_TABLE_TYPE);
    }

    private String buildCreate(String variantsTableName, String columnFamily, PTableType tableType) {
        StringBuilder sb = new StringBuilder().append("CREATE ").append(tableType).append(" IF NOT EXISTS ")
                .append(phoenixHelper.getEscapedFullTableName(tableType, variantsTableName)).append(' ').append('(');
        for (VariantColumn variantColumn : VariantColumn.values()) {
            switch (variantColumn) {
                case CHROMOSOME:
                case POSITION:
                    sb.append(" \"").append(variantColumn).append("\" ").append(variantColumn.sqlType()).append(" NOT NULL , ");
                    break;
                default:
                    sb.append(" \"").append(variantColumn).append("\" ").append(variantColumn.sqlType()).append(" , ");
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
        return Arrays.asList(
                getStatsColumn(studyId, cohortId),
                getStatsFreqColumn(studyId, cohortId),
                getStatsMafColumn(studyId, cohortId),
                getStatsMgfColumn(studyId, cohortId),
                getStatsPassFreqColumn(studyId, cohortId));
    }

    public static Column getStatsColumn(int studyId, int cohortId) {
        return Column.build(studyId + COLUMN_KEY_SEPARATOR_STR + cohortId + COHORT_STATS_PROTOBUF_SUFFIX, PVarbinary.INSTANCE);
    }

    public static Column getStudyColumn(int studyId) {
        return Column.build(String.valueOf(studyId) + STUDY_SUFIX, PUnsignedInt.INSTANCE);
    }

    public static int extractStudyId(String columnKey) {
        return extractStudyId(columnKey, true);
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

    public static Integer extractSampleIdOrNull(byte[] columnValue, int offset, int length) {
        if (AbstractPhoenixConverter.endsWith(columnValue, offset, length, SAMPLE_DATA_SUFIX_BYTES)) {
            return extractId(Bytes.toString(columnValue, offset, length), false, "sample");
        } else {
            return null;
        }
    }

    public static int extractSampleId(String columnKey) {
        return extractSampleId(columnKey, true);
    }

    public static Integer extractSampleId(String columnKey, boolean failOnMissing) {
        if (columnKey.endsWith(SAMPLE_DATA_SUFIX)) {
            return extractId(columnKey, failOnMissing, "sample");
        } else if (failOnMissing) {
            throw new IllegalArgumentException("Not a sample column: " + columnKey);
        } else {
            return null;
        }
    }

    public static int extractFileId(String columnKey) {
        return extractFileId(columnKey, true);
    }

    public static Integer extractFileId(String columnKey, boolean failOnMissing) {
        if (columnKey.endsWith(FILE_SUFIX)) {
            return extractId(columnKey, failOnMissing, "file");
        } else if (failOnMissing) {
            throw new IllegalArgumentException("Not a file column: " + columnKey);
        } else {
            return null;
        }
    }

    public static Integer extractFileIdOrNull(byte[] columnValue, int offset, int length) {
        if (AbstractPhoenixConverter.endsWith(columnValue, offset, length, FILE_SUFIX_BYTES)) {
            return extractId(Bytes.toString(columnValue, offset, length), false, "file");
        } else {
            return null;
        }
    }

    public static int extractCohortStatsId(String columnKey) {
        return extractCohortStatsId(columnKey, true);
    }

    public static Integer extractCohortStatsId(String columnKey, boolean failOnMissing) {
        if (columnKey.endsWith(COHORT_STATS_PROTOBUF_SUFFIX)) {
            return extractId(columnKey, failOnMissing, "cohortStats");
        } else if (failOnMissing) {
            throw new IllegalArgumentException("Not a file column: " + columnKey);
        } else {
            return null;
        }
    }

    public static Integer extractCohortStatsIdOrNull(byte[] columnValue, int offset, int length) {
        if (AbstractPhoenixConverter.endsWith(columnValue, offset, length, COHORT_STATS_PROTOBUF_SUFFIX_BYTES)) {
            return extractId(Bytes.toString(columnValue, offset, length), false, "cohortStats");
        } else {
            return null;
        }
    }

    public static int extractScoreId(String columnKey) {
        return extractScoreId(columnKey, true);
    }

    public static Integer extractScoreId(String columnKey, boolean failOnMissing) {
        if (columnKey.endsWith(VARIANT_SCORE_SUFIX)) {
            return extractId(columnKey, failOnMissing, "score");
        } else if (failOnMissing) {
            throw new IllegalArgumentException("Not a file column: " + columnKey);
        } else {
            return null;
        }
    }

    public static Integer extractScoreIdOrNull(byte[] columnValue, int offset, int length) {
        if (AbstractPhoenixConverter.endsWith(columnValue, offset, length, VARIANT_SCORE_SUFIX_BYTES)) {
            return extractId(Bytes.toString(columnValue, offset, length), false, "score");
        } else {
            return null;
        }
    }

    private static Integer extractId(String columnKey, boolean failOnMissing, String idType) {
        int startIndex = columnKey.indexOf(COLUMN_KEY_SEPARATOR);
        int endIndex = columnKey.lastIndexOf(COLUMN_KEY_SEPARATOR);
        if (startIndex != endIndex && startIndex > 0) {
            String id = columnKey.substring(startIndex + 1, endIndex);
            if (StringUtils.isNotBlank(columnKey)
                    && Character.isDigit(columnKey.charAt(0))
                    && StringUtils.isNumeric(id)) {
                return Integer.parseInt(id);
            }
        }
        if (failOnMissing) {
            throw new IllegalStateException("Integer expected for " + idType + " ID from " + columnKey);
        } else {
            return null;
        }
    }

    public static StringBuilder buildStudyColumnsPrefix(int studyId, StringBuilder stringBuilder) {
        return stringBuilder.append(studyId).append(COLUMN_KEY_SEPARATOR);
    }

    public static String buildStudyColumnsPrefix(int studyId) {
        return studyId + "_";
    }

    public static Column getStatsFreqColumn(int studyId, int cohortId) {
        return Column.build(buildStudyColumnsPrefix(studyId) + cohortId + COHORT_STATS_FREQ_SUFFIX, PFloatArray.INSTANCE);
    }

    public static Column getStatsMafColumn(int studyId, int cohortId) {
        return Column.build(buildStudyColumnsPrefix(studyId) + cohortId + COHORT_STATS_MAF_SUFFIX, PFloat.INSTANCE);
    }

    public static Column getStatsMgfColumn(int studyId, int cohortId) {
        return Column.build(buildStudyColumnsPrefix(studyId) + cohortId + COHORT_STATS_MGF_SUFFIX, PFloat.INSTANCE);
    }

    public static Column getStatsPassFreqColumn(int studyId, int cohortId) {
        return Column.build(buildStudyColumnsPrefix(studyId) + cohortId + COHORT_STATS_PASS_FREQ_SUFFIX, PFloat.INSTANCE);
    }

    public static byte[] buildSampleColumnKey(int studyId, int sampleId) {
        return Bytes.toBytes(buildSampleColumnKey(studyId, sampleId, new StringBuilder()).toString());
    }

    public static StringBuilder buildSampleColumnKey(int studyId, int sampleId, StringBuilder stringBuilder) {
        return buildStudyColumnsPrefix(studyId, stringBuilder).append(sampleId).append(SAMPLE_DATA_SUFIX);
    }

    public static Column getSampleColumn(int studyId, int sampleId) {
        return Column.build(buildSampleColumnKey(studyId, sampleId, new StringBuilder()).toString(), PVarcharArray.INSTANCE);
    }

    public static boolean isSampleCell(Cell cell) {
        return AbstractPhoenixConverter.endsWith(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                VariantPhoenixHelper.SAMPLE_DATA_SUFIX_BYTES);
    }

    public static byte[] buildFileColumnKey(int studyId, int fileId) {
        return Bytes.toBytes(buildFileColumnKey(studyId, fileId, new StringBuilder()).toString());
    }

    public static StringBuilder buildFileColumnKey(int studyId, int fileId, StringBuilder stringBuilder) {
        return buildStudyColumnsPrefix(studyId, stringBuilder).append(fileId).append(FILE_SUFIX);
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

    public static Column getVariantScoreColumn(int studyID, int scoreId) {
        String columnName = buildStudyColumnsPrefix(studyID, new StringBuilder()).append(scoreId).append(VARIANT_SCORE_SUFIX).toString();
        return Column.build(columnName, PFloatArray.INSTANCE, false);
    }
}
