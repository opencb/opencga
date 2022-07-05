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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.*;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper.Column;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.ANNOT_CONSERVATION;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.ANNOT_FUNCTIONAL_SCORE;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.*;

/**
 * Created on 15/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class VariantPhoenixSchema {

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

    public static final int COHORT_STATS_COLUMNS_PER_COHORT = 5;

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

    protected static Logger logger = LoggerFactory.getLogger(VariantPhoenixSchema.class);


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
        CLINICAL(ANNOTATION_PREFIX + "CLI", PVarcharArray.INSTANCE),
        @Deprecated
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
        // No mapping between 1kg_phase3 (CB4) and 1000G (CB5)
        mappingPopulationStudies.put("1000GENOMES_PHASE_3", ParamConstants.POP_FREQ_1000G_CB_V4.toUpperCase());
        mappingPopulationStudies.put("ESP_6500", "ESP6500");
        MAPPING_POPULATION_SUDIES = Collections.unmodifiableMap(mappingPopulationStudies);


        List<Column> humanPopulationFrequenciesColumns = new ArrayList<>(Arrays.asList(
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

        Stream.of("ALL", "AFR", "AMR", "EAS", "EUR", "SAS", "ACB", "ASW", "BEB",
                "CDX", "CEU", "CHB", "CHD", "CHS", "CLM", "ESN", "FIN", "GBR",
                "GIH", "GWD", "IBS", "ITU", "JPT", "KHV", "LWK", "MSL", "MXL",
                "PEL", "PJL", "PUR", "STU", "TSI", "YRI").forEach(pop -> {
            humanPopulationFrequenciesColumns.add(getPopulationFrequencyColumn(ParamConstants.POP_FREQ_1000G_CB_V4, pop));
            humanPopulationFrequenciesColumns.add(getPopulationFrequencyColumn(ParamConstants.POP_FREQ_1000G_CB_V5, pop));
        });
        HUMAN_POPULATION_FREQUENCIES_COLUMNS = Collections.unmodifiableList(humanPopulationFrequenciesColumns);

        DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            getPopulationFrequencyColumn(ParamConstants.POP_FREQ_1000G_CB_V5, "ALL"),
            getPopulationFrequencyColumn(ParamConstants.POP_FREQ_1000G_CB_V5, "AFR"),
            getPopulationFrequencyColumn(ParamConstants.POP_FREQ_1000G_CB_V5, "AMR"),
            getPopulationFrequencyColumn(ParamConstants.POP_FREQ_1000G_CB_V5, "EAS"),
            getPopulationFrequencyColumn(ParamConstants.POP_FREQ_1000G_CB_V5, "EUR"),
            getPopulationFrequencyColumn(ParamConstants.POP_FREQ_1000G_CB_V5, "SAS"),

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

    private VariantPhoenixSchema() {
    }

    public static List<Column> getStudyColumns(int studyId) {
        return Arrays.asList(getStudyColumn(studyId), getFillMissingColumn(studyId));
    }

    public static List<Column> getReleaseColumns(int release) {
        List<Column> columns = new ArrayList<>(release);
        for (int i = 1; i <= release; i++) {
            columns.add(getReleaseColumn(i));
        }
        return columns;
    }

    public static List<PhoenixHelper.Index> getPopFreqIndices(String variantsTableName) {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        return Arrays.asList(getPopFreqIndex(variantsTableName, ParamConstants.POP_FREQ_1000G, "ALL"),
                getPopFreqIndex(variantsTableName, "EXAC", "ALL"));
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

    static List<PhoenixHelper.Index> getIndices(String variantsTableName) {
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

    public static List<Column> getStatsColumns(int studyId, List<Integer> cohortIds) {
        List<Column> columns = new ArrayList<>(cohortIds.size() * 5);
        for (Integer cohortId : cohortIds) {
            columns.addAll(getStatsColumns(studyId, cohortId));
        }
        return columns;
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
        if (isSampleDataColumn(columnKey)) {
            return extractId(columnKey, failOnMissing, "sample");
        } else if (failOnMissing) {
            throw new IllegalArgumentException("Not a sample column: " + columnKey);
        } else {
            return null;
        }
    }

    public static int extractFileIdFromSampleColumn(String columnKey) {
        return extractFileIdFromSampleColumn(columnKey, true);
    }

    public static Integer extractFileIdFromSampleColumn(String columnKey, boolean failOnMissing) {
        if (isSampleDataColumn(columnKey) && StringUtils.countMatches(columnKey, COLUMN_KEY_SEPARATOR) == 3) {
            return extractId(columnKey, failOnMissing, "sample", columnKey.indexOf(COLUMN_KEY_SEPARATOR) + 1);
        } else if (failOnMissing) {
            throw new IllegalArgumentException("Not a sample column: " + columnKey);
        } else {
            return null;
        }
    }

    public static boolean isSampleDataColumn(String columnKey) {
        return columnKey.endsWith(SAMPLE_DATA_SUFIX);
    }

    public static boolean isFileColumn(String columnKey) {
        return columnKey.endsWith(FILE_SUFIX);
    }

    public static int extractFileId(String columnKey) {
        return extractFileId(columnKey, true);
    }

    public static Integer extractFileId(String columnKey, boolean failOnMissing) {
        if (isFileColumn(columnKey)) {
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
        return extractId(columnKey, failOnMissing, idType, 0);
    }

    private static Integer extractId(String columnKey, boolean failOnMissing, String idType, int fromIndex) {
        int startIndex = columnKey.indexOf(COLUMN_KEY_SEPARATOR, fromIndex);
        int endIndex = columnKey.indexOf(COLUMN_KEY_SEPARATOR, startIndex + 1);
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

    public static byte[] buildSampleColumnKey(int studyId, int sampleId, int fileId) {
        return Bytes.toBytes(buildSampleColumnKey(studyId, sampleId, fileId, new StringBuilder()).toString());
    }

    public static StringBuilder buildSampleColumnKey(int studyId, int sampleId, StringBuilder stringBuilder) {
        return buildStudyColumnsPrefix(studyId, stringBuilder).append(sampleId).append(SAMPLE_DATA_SUFIX);
    }

    public static StringBuilder buildSampleColumnKey(int studyId, int sampleId, int fileId, StringBuilder stringBuilder) {
        return buildStudyColumnsPrefix(studyId, stringBuilder)
                .append(sampleId).append(COLUMN_KEY_SEPARATOR).append(fileId).append(SAMPLE_DATA_SUFIX);
    }

    public static List<Column> getSampleColumns(SampleMetadata sampleMetadata) {
        return getSampleColumns(sampleMetadata, null);
    }

    public static List<Column> getSampleColumns(SampleMetadata sampleMetadata, Collection<Integer> requiredFiles) {
        return getSampleColumns(sampleMetadata.getStudyId(), sampleMetadata.getId(), sampleMetadata.getFiles(), requiredFiles,
                sampleMetadata.getSplitData());
    }

    public static List<Column> getSampleColumns(int studyId, int sampleId, List<Integer> files, Collection<Integer> requiredFiles,
                                                VariantStorageEngine.SplitData splitData) {
        List<Column> columns = new ArrayList<>(1);
        if (VariantStorageEngine.SplitData.MULTI.equals(splitData)) {
            if (requiredFiles == null || requiredFiles.contains(files.get(0))) {
                columns.add(getSampleColumn(studyId, sampleId));
            }
            for (Integer file : files.subList(1, files.size())) {
                if (requiredFiles == null || requiredFiles.contains(file)) {
                    columns.add(getSampleColumn(studyId, sampleId, file));
                }
            }
        } else {
            // Required files doesn't apply for SplitData!=MULTI
            columns.add(getSampleColumn(studyId, sampleId));
        }
        return columns;
    }

    protected static Column getSampleColumn(int studyId, int sampleId) {
        return Column.build(buildSampleColumnKey(studyId, sampleId, new StringBuilder()).toString(), PVarcharArray.INSTANCE);
    }

    protected static Column getSampleColumn(int studyId, int sampleId, int fileId) {
        return Column.build(buildSampleColumnKey(studyId, sampleId, fileId, new StringBuilder()).toString(), PVarcharArray.INSTANCE);
    }

    public static boolean isSampleCell(Cell cell) {
        return AbstractPhoenixConverter.endsWith(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                VariantPhoenixSchema.SAMPLE_DATA_SUFIX_BYTES);
    }

    public static byte[] buildFileColumnKey(int studyId, int fileId) {
        return Bytes.toBytes(buildFileColumnKey(studyId, fileId, new StringBuilder()).toString());
    }

    public static StringBuilder buildFileColumnKey(int studyId, int fileId, StringBuilder stringBuilder) {
        return buildStudyColumnsPrefix(studyId, stringBuilder).append(fileId).append(FILE_SUFIX);
    }

    public static Column getFileColumn(int studyId, int fileId) {
        return Column.build(buildFileColumnKey(studyId, fileId, new StringBuilder()).toString(), PVarcharArray.INSTANCE);
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
