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

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsTask;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryOptions.COUNT;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper.Column;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.*;

/**
 * Created on 16/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSqlQueryParser {

    public static final List<String> DEFAULT_LOADED_GENOTYPES = Collections.unmodifiableList(Arrays.asList(
            ".", "./.",
            "0/0", "0|0",
            "0/1", "1/0", "1/1",
            "0/2", "1/2", "2/2",
            "0/3", "1/3", "2/3", "3/3",
            ".|.",
            "0|1", "1|0", "1|1",
            "0|2", "2|0", "2|1", "1|2", "2|2",
            "0|3", "1|3", "2|3", "3|3",
            "3|0", "3|1", "3|2"));
    private final GenomeHelper genomeHelper;
    private final String variantTable;
    private final Logger logger = LoggerFactory.getLogger(VariantSqlQueryParser.class);
    private final StudyConfigurationManager studyConfigurationManager;
    private final boolean clientSideSkip;

    private static final Map<String, String> SQL_OPERATOR;

    static {
        SQL_OPERATOR = new HashMap<>();
        SQL_OPERATOR.put("==", "=");
        SQL_OPERATOR.put("=~", "LIKE");
        SQL_OPERATOR.put("~", "LIKE");
        SQL_OPERATOR.put("!", "!=");
    }

    // Internal usage only
    private static final String ALT_GT = "ALT";

    public static class VariantPhoenixSQLQuery {
        private String sql;
        private SelectVariantElements select;

        public String getSql() {
            return sql;
        }

        public SelectVariantElements getSelect() {
            return select;
        }
    }

    public VariantSqlQueryParser(GenomeHelper genomeHelper, String variantTable, StudyConfigurationManager studyConfigurationManager) {
        this(genomeHelper, variantTable, studyConfigurationManager, false);
    }

    public VariantSqlQueryParser(GenomeHelper genomeHelper, String variantTable,
                                 StudyConfigurationManager studyConfigurationManager, boolean clientSideSkip) {
        this.genomeHelper = genomeHelper;
        this.variantTable = variantTable;
        this.studyConfigurationManager = studyConfigurationManager;
        this.clientSideSkip = clientSideSkip;
    }

    public VariantPhoenixSQLQuery parse(Query query, QueryOptions options) {

        StringBuilder sb = new StringBuilder("SELECT ");
        VariantPhoenixSQLQuery phoenixSQLQuery = new VariantPhoenixSQLQuery();

        try {

            Set<Column> dynamicColumns = new HashSet<>();
            List<String> regionFilters = getRegionFilters(query);
            List<String> filters = getOtherFilters(query, options, dynamicColumns);

            List<HintNode.Hint> hints = new ArrayList<>();
            if (DEFAULT_TABLE_TYPE != PTableType.VIEW && filters.isEmpty()) {
                // Only region filters. Hint no index usage
                hints.add(HintNode.Hint.NO_INDEX);
            }
            if (options.containsKey("HINT")) {
                for (String hint : options.getAsStringList("HINT")) {
                    hints.add(HintNode.Hint.valueOf(hint));
                }
            }
            if (!hints.isEmpty()) {
                sb.append("/*+ ").append(hints.stream().map(Object::toString).collect(Collectors.joining(","))).append(" */ ");
            }

            appendProjectedColumns(sb, query, options, phoenixSQLQuery);
            appendFromStatement(sb, dynamicColumns);
            appendWhereStatement(sb, regionFilters, filters);

        } catch (VariantQueryException e) {
            e.setQuery(query);
            throw e;
        }

        if (options.getBoolean(QueryOptions.SORT)) {
            sb.append(" ORDER BY ").append(VariantColumn.CHROMOSOME.column()).append(',').append(VariantColumn.POSITION.column());

            String order = options.getString(QueryOptions.ORDER, QueryOptions.ASCENDING);
            if (order.equalsIgnoreCase(QueryOptions.ASCENDING) || order.equalsIgnoreCase("ASC")) {
                sb.append(" ASC ");
            } else {
                sb.append(" DESC ");
            }
        }

        if (clientSideSkip) {
            int skip = Math.max(0, options.getInt(QueryOptions.SKIP));
            if (options.getInt(QueryOptions.LIMIT) > 0) {
                sb.append(" LIMIT ").append(skip + options.getInt(QueryOptions.LIMIT));
            }
        } else {
            if (options.getInt(QueryOptions.LIMIT) > 0) {
                sb.append(" LIMIT ").append(options.getInt(QueryOptions.LIMIT));
            }

            if (options.getInt(QueryOptions.SKIP) > 0) {
                sb.append(" OFFSET ").append(options.getInt(QueryOptions.SKIP));
            }
        }

        phoenixSQLQuery.sql = sb.toString();
        return phoenixSQLQuery;
    }

    /**
     * Select only the required columns.
     * <p>
     * Uses the params:
     * {@link VariantQueryParam#INCLUDE_STUDY}
     * {@link VariantQueryParam#INCLUDE_SAMPLE}
     * {@link VariantQueryParam#INCLUDE_FILE}
     * {@link VariantQueryParam#UNKNOWN_GENOTYPE}
     *
     * @param sb              SQLStringBuilder
     * @param query           Query to parse
     * @param options         other options
     * @param phoenixSQLQuery VariantPhoenixSQLQuery
     * @return String builder
     */
    protected StringBuilder appendProjectedColumns(StringBuilder sb, Query query, QueryOptions options,
                                                   VariantPhoenixSQLQuery phoenixSQLQuery) {
        if (options.getBoolean(COUNT)) {
            return sb.append(" COUNT(*) ");
        } else {
            SelectVariantElements selectVariantElements = parseSelectElements(query, options, studyConfigurationManager);
            phoenixSQLQuery.select = selectVariantElements;
            Set<VariantField> returnedFields = selectVariantElements.getFields();
            Map<Integer, List<Integer>> returnedSamples = selectVariantElements.getSamples();
            Collection<Integer> studyIds = selectVariantElements.getStudies();

            sb.append(VariantColumn.CHROMOSOME).append(',')
                    .append(VariantColumn.POSITION).append(',')
                    .append(VariantColumn.REFERENCE).append(',')
                    .append(VariantColumn.ALTERNATE).append(',')
                    .append(VariantColumn.TYPE);

            if (returnedFields.contains(VariantField.STUDIES)) {
                for (Integer studyId : studyIds) {
                    StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, null).first();
                    Column studyColumn = VariantPhoenixHelper.getStudyColumn(studyId);
                    sb.append(",\"").append(studyColumn.column()).append('"');
                    sb.append(",\"").append(VariantPhoenixHelper.getFillMissingColumn(studyId).column()).append('"');
                    if (returnedFields.contains(VariantField.STUDIES_STATS)) {
                        for (Integer cohortId : studyConfiguration.getCalculatedStats()) {
                            Column statsColumn = getStatsColumn(studyId, cohortId);
                            sb.append(",\"").append(statsColumn.column()).append('"');
                        }
                    }
                }
            }
            if (returnedFields.contains(VariantField.STUDIES_FILES)) {
                selectVariantElements.getFiles().forEach((studyId, fileIds) -> {
                    for (Integer fileId : fileIds) {
                        sb.append(",\"");
                        buildFileColumnKey(studyId, fileId, sb);
                        sb.append('"');
                    }
                });
            }
            if (returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)) {
                returnedSamples.forEach((studyId, sampleIds) -> {
                    for (Integer sampleId : sampleIds) {
                        sb.append(",\"");
                        buildSampleColumnKey(studyId, sampleId, sb);
                        sb.append('"');
                    }
                });
            }
            if (returnedFields.contains(VariantField.ANNOTATION)) {
                sb.append(',').append(VariantColumn.FULL_ANNOTATION);

                int release = studyConfigurationManager.getProjectMetadata().first().getRelease();
                for (int i = 1; i <= release; i++) {
                    sb.append(',');
                    VariantPhoenixHelper.buildReleaseColumnKey(i, sb);
                }
            }

            return sb;
        }
    }

    protected void appendFromStatement(StringBuilder sb, Set<Column> dynamicColumns) {
        sb.append(" FROM ").append(getEscapedFullTableName(variantTable, genomeHelper.getConf()));

        if (!dynamicColumns.isEmpty()) {
            sb.append(dynamicColumns.stream()
                    .map(column -> SchemaUtil.getEscapedFullColumnName(column.column()) + " " + column.sqlType())
                    .collect(Collectors.joining(",", " ( ", " ) "))
            );
        }

    }

    protected StringBuilder appendWhereStatement(StringBuilder sb, List<String> regionFilters, List<String> filters) {
        if (!regionFilters.isEmpty() || !filters.isEmpty()) {
            sb.append(" WHERE");
        }

        appendFilters(sb, regionFilters, QueryOperation.OR);

        if (!filters.isEmpty() && !regionFilters.isEmpty()) {
            sb.append(" AND");
        }

        appendFilters(sb, filters, QueryOperation.AND);

        return sb;
    }

    protected String appendFilters(List<String> filters, QueryOperation logicalOperation) {
        return appendFilters(new StringBuilder(), filters, logicalOperation).toString();
    }

    protected StringBuilder appendFilters(StringBuilder sb, List<String> filters, QueryOperation logicalOperation) {
        String delimiter;
        if (logicalOperation == null) {
            if (filters.size() == 1) {
                return sb.append(" ( ").append(filters.get(0)).append(" ) ");
            } else {
                throw new VariantQueryException("Missing logical operation!");
            }
        }
        switch (logicalOperation) {
            case AND:
                delimiter = " ) AND ( ";
                break;
            case OR:
                delimiter = " ) OR ( ";
                break;
            default:
                throw new IllegalArgumentException("Unknown QueryOperation " + logicalOperation);
        }
        if (!filters.isEmpty()) {
            sb.append(filters.stream().collect(Collectors.joining(delimiter, " ( ( ", " ) )")));
        }
        return sb;
    }

    /**
     * Transform QueryParams that are inclusive.
     *
     * A variant will pass this filters if matches with ANY of this filters.
     *
     * {@link VariantQueryParam#REGION}
     *
     * Using annotation:
     * {@link VariantQueryParam#ID}
     * {@link VariantQueryParam#GENE}
     * {@link VariantQueryParam#ANNOT_XREF}
     * {@link VariantQueryParam#ANNOT_CLINVAR}
     * {@link VariantQueryParam#ANNOT_COSMIC}
     *
     * @param query Query to parse
     * @return List of region filters
     */
    protected List<String> getRegionFilters(Query query) {
        List<String> regionFilters = new LinkedList<>();

        if (isValidParam(query, REGION)) {
            List<Region> regions = Region.parseRegions(query.getString(REGION.key()), true);
            for (Region region : regions) {
                regionFilters.add(getRegionFilter(region));
            }
        }

        VariantQueryXref variantQueryXref = VariantQueryUtils.parseXrefs(query);

        // TODO: This should filter by ID from the VCF
        for (String id : variantQueryXref.getIds()) {
            regionFilters.add(buildFilter(VariantColumn.XREFS, "=", id));
        }
        for (String xrefs : variantQueryXref.getOtherXrefs()) {
            regionFilters.add(buildFilter(VariantColumn.XREFS, "=", xrefs));
        }
        if (!variantQueryXref.getVariants().isEmpty()) {
            regionFilters.add(getVariantFilter(variantQueryXref.getVariants()));
        }

        // Ask cellbase for gene region?
        if (!variantQueryXref.getGenes().isEmpty()) {
            if (isValidParam(query, ANNOT_GENE_REGIONS)) {
                for (Region region : Region.parseRegions(query.getString(ANNOT_GENE_REGIONS.key()))) {
                    regionFilters.add(getRegionFilter(region));
                }
            } else {
                throw new VariantQueryException("Error building query by genes '" + variantQueryXref.getGenes()
                        + "', missing gene regions");
            }
        }

//        if (regionFilters.isEmpty()) {
//            // chromosome != _METADATA
//            regionFilters.add(VariantColumn.CHROMOSOME + " != '" + genomeHelper.getMetaRowKeyString() + "'");
//        }
        return regionFilters;
    }

    private String getVariantFilter(List<Variant> variants) {
        StringBuilder sb = new StringBuilder().append("(")
                .append(VariantColumn.CHROMOSOME).append(", ")
                .append(VariantColumn.POSITION).append(", ")
                .append(VariantColumn.REFERENCE).append(", ")
                .append(VariantColumn.ALTERNATE).append(") IN (");
        Iterator<Variant> iterator = variants.iterator();
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            sb.append("('").append(checkStringValue(variant.getChromosome())).append("', ")
                    .append(variant.getStart()).append(", ")
                    .append('\'').append(checkStringValue(variant.getReference())).append("', ")
                    .append('\'').append(checkStringValue(variant.getAlternate())).append("') ");
            if (iterator.hasNext()) {
                sb.append(',');
            }
        }
        sb.append(')');
        return sb.toString();
    }

//    private String getRegionFilter(Region region) {
//        if (region.getStart() == region.getEnd()) {
//            return String.format("(%s,%s) = ('%s',%s)",
//                    VariantColumn.CHROMOSOME,
//                    VariantColumn.POSITION,
//                    region.getChromosome(), region.getStart());
//        } else {
//            return String.format("(%s,%s) BETWEEN ('%s',%s) AND ('%s',%s)",
//                    VariantColumn.CHROMOSOME,
//                    VariantColumn.POSITION,
//                    region.getChromosome(), region.getStart(),
//                    region.getChromosome(), region.getEnd());
//        }
//    }

    private String getRegionFilter(Region region) {
        List<String> subFilters = new ArrayList<>(3);
        subFilters.add(buildFilter(VariantColumn.CHROMOSOME, "=", region.getChromosome()));
        if (region.getStart() > 1) {
            subFilters.add(buildFilter(VariantColumn.POSITION, ">=", region.getStart()));
        }
        if (region.getEnd() < Integer.MAX_VALUE) {
            subFilters.add(buildFilter(VariantColumn.POSITION, "<=", region.getEnd()));
        }
        return appendFilters(subFilters, QueryOperation.AND);
    }

    /**
     * Transform QueryParams that are exclusive.
     *
     * A variant will pass this filters if matches with ALL of this filters.
     *
     * Variant filters:
     * {@link VariantQueryParam#REFERENCE}
     * {@link VariantQueryParam#ALTERNATE}
     * {@link VariantQueryParam#TYPE}
     * {@link VariantQueryParam#STUDY}
     * {@link VariantQueryParam#FILE}
     * {@link VariantQueryParam#FILTER}
     * {@link VariantQueryParam#QUAL}
     * {@link VariantQueryParam#COHORT}
     * {@link VariantQueryParam#GENOTYPE}
     * {@link VariantQueryParam#RELEASE}
     *
     * Annotation filters:
     * {@link VariantQueryParam#ANNOTATION_EXISTS}
     * {@link VariantQueryParam#ANNOT_CONSEQUENCE_TYPE}
     * {@link VariantQueryParam#ANNOT_BIOTYPE}
     * {@link VariantQueryParam#ANNOT_POLYPHEN}
     * {@link VariantQueryParam#ANNOT_SIFT}
     * {@link VariantQueryParam#ANNOT_CONSERVATION}
     * {@link VariantQueryParam#ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY}
     * {@link VariantQueryParam#ANNOT_POPULATION_ALTERNATE_FREQUENCY}
     * {@link VariantQueryParam#ANNOT_POPULATION_REFERENCE_FREQUENCY}

     * {@link VariantQueryParam#ANNOT_TRANSCRIPTION_FLAG}
     * {@link VariantQueryParam#ANNOT_GENE_TRAIT_ID}
     * {@link VariantQueryParam#ANNOT_GENE_TRAIT_NAME}
     * {@link VariantQueryParam#ANNOT_HPO}
     * {@link VariantQueryParam#ANNOT_GO}
     * {@link VariantQueryParam#ANNOT_EXPRESSION}
     * {@link VariantQueryParam#ANNOT_PROTEIN_KEYWORD}
     * {@link VariantQueryParam#ANNOT_DRUG}
     * {@link VariantQueryParam#ANNOT_FUNCTIONAL_SCORE}
     *
     * Stats filters:
     * {@link VariantQueryParam#STATS_MAF}
     * {@link VariantQueryParam#STATS_MGF}
     * {@link VariantQueryParam#MISSING_ALLELES}
     * {@link VariantQueryParam#MISSING_GENOTYPES}
     *
     * @param query     Query to parse
     * @param options   Options
     * @param dynamicColumns Initialized empty set to be filled with dynamic columns required by the queries
     * @return List of sql filters
     */
    protected List<String> getOtherFilters(Query query, QueryOptions options, final Set<Column> dynamicColumns) {
        List<String> filters = new LinkedList<>();

        // Variant filters:
        StudyConfiguration defaultStudyConfiguration = addVariantFilters(query, options, filters);

        // Annotation filters:
        addAnnotFilters(query, dynamicColumns, filters);

        // Stats filters:
        addStatsFilters(query, defaultStudyConfiguration, filters);

        return filters;
    }

    protected StudyConfiguration addVariantFilters(Query query, QueryOptions options, List<String> filters) {
        addQueryFilter(query, REFERENCE, VariantColumn.REFERENCE, filters);

        addQueryFilter(query, ALTERNATE, VariantColumn.ALTERNATE, filters);

        addQueryFilter(query, TYPE, VariantColumn.TYPE, filters, s -> {
            VariantType type = VariantType.valueOf(s);
            Set<VariantType> subTypes = Variant.subTypes(type);
            ArrayList<VariantType> types = new ArrayList<>(subTypes.size() + 1);
            types.add(type);
            types.addAll(subTypes);
            return types;
        });

        final StudyConfiguration defaultStudyConfiguration;
        if (isValidParam(query, STUDY)) {
            String value = query.getString(STUDY.key());
            QueryOperation operation = checkOperator(value);
            List<String> values = splitValue(value, operation);
            StringBuilder sb = new StringBuilder();
            Iterator<String> iterator = values.iterator();
            Map<String, Integer> studies = studyConfigurationManager.getStudies(options);
            Set<Integer> notNullStudies = new HashSet<>();
            while (iterator.hasNext()) {
                String study = iterator.next();
                Integer studyId = studyConfigurationManager.getStudyId(study, false, studies);
                if (isNegated(study)) {
                    sb.append("\"").append(getStudyColumn(studyId).column()).append("\" IS NULL ");
                } else {
                    notNullStudies.add(studyId);
                    sb.append("\"").append(getStudyColumn(studyId).column()).append("\" IS NOT NULL ");
                }
                if (iterator.hasNext()) {
                    if (operation == null || operation.equals(QueryOperation.AND)) {
                        sb.append(" AND ");
                    } else {
                        sb.append(" OR ");
                    }
                }
            }
            // Skip this filter if contains all the existing studies.
            if (studies.values().size() != notNullStudies.size() || !notNullStudies.containsAll(studies.values())) {
                filters.add(sb.toString());
            }
            List<Integer> studyIds = studyConfigurationManager.getStudyIds(values, options);
            if (studyIds.size() == 1) {
                defaultStudyConfiguration = studyConfigurationManager.getStudyConfiguration(studyIds.get(0), options).first();
            } else {
                defaultStudyConfiguration = null;
            }
        } else {
            List<Integer> studyIds = studyConfigurationManager.getStudyIds(options);
            if (studyIds.size() == 1) {
                defaultStudyConfiguration = studyConfigurationManager.getStudyConfiguration(studyIds.get(0), options).first();
            } else {
                defaultStudyConfiguration = null;
            }
//            StringBuilder sb = new StringBuilder();
//            for (Iterator<Integer> iterator = studyIds.iterator(); iterator.hasNext();) {
//                Integer studyId = iterator.next();
//                sb.append('"').append(getStudyColumn(studyId).column()).append("\" IS NOT NULL");
//                if (iterator.hasNext()) {
//                    sb.append(" OR ");
//                }
//            }
//            filters.add(sb.toString());
        }

        QueryOperation filtersOperation = null;
        List<String> filterValues = Collections.emptyList();
        if (isValidParam(query, FILTER)) {
            String value = query.getString(FILTER.key());
            filtersOperation = checkOperator(value);
            filterValues = splitValue(value, filtersOperation);
            if (!filterValues.isEmpty()) {
                if (!isValidParam(query, FILE)) {
                    throw VariantQueryException.malformedParam(FILTER, value, "Missing \"" + FILE.key() + "\" filter");
                }
            }
        }

        QueryOperation qualOperation = null;
        List<String> qualValues = Collections.emptyList();
        if (isValidParam(query, QUAL)) {
            String value = query.getString(QUAL.key());
            qualOperation = checkOperator(value);
            qualValues = splitValue(value, qualOperation);
            if (!qualValues.isEmpty()) {
                if (!isValidParam(query, FILE)) {
                    throw VariantQueryException.malformedParam(QUAL, value, "Missing \"" + FILE.key() + "\" filter");
                }
            }
        }

        if (isValidParam(query, FILE)) {
            String value = query.getString(FILE.key());
            QueryOperation operation = checkOperator(value);
            List<String> values = splitValue(value, operation);


            StringBuilder sb = new StringBuilder();
            for (Iterator<String> iterator = values.iterator(); iterator.hasNext();) {
                String file = iterator.next();
                Pair<Integer, Integer> fileIdPair = studyConfigurationManager.getFileIdPair(file, false, defaultStudyConfiguration);

                sb.append(" ( ");
                if (isNegated(file)) {
                    // ( "FILE" IS NULL OR "FILE"[3] != 'N' )

                    sb.append('"');
                    buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                    sb.append("\" IS NULL ");

                    sb.append(" OR ");

                    sb.append('"');
                    buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                    sb.append('"');
                    // Arrays in SQL are 1-based.
                    sb.append('[').append(HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX + 1).append(']');
                    sb.append(" != '").append(VariantOverlappingStatus.NONE.toString()).append('\'');

                } else {
                    // ( "FILE"[3] = 'N' )
                    sb.append('"');
                    buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                    sb.append('"');
                    // Arrays in SQL are 1-based.
                    sb.append('[').append(HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX + 1).append(']');
                    sb.append(" = '").append(VariantOverlappingStatus.NONE.toString()).append('\'');

                    if (!filterValues.isEmpty()) {
                        // ( "FILE"[3] = 'N' AND ( "FILE"[5] = 'FILTER_1' OR "FILE"[5] = 'FILTER_2' ) )
                        sb.append(" AND ( ");
                        for (int i = 0; i < filterValues.size(); i++) {
                            String filter = checkStringValue(filterValues.get(i));
                            boolean negated = isNegated(filter);
                            if (negated) {
                                filter = removeNegation(filter);
                            }

                            if (i > 0 && filtersOperation != null) {
                                sb.append(' ').append(filtersOperation.name()).append(' ');
                            }

                            sb.append('"');
                            buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                            sb.append('"');

                            // Arrays in SQL are 1-based.
                            sb.append('[').append(HBaseToStudyEntryConverter.FILE_FILTER_IDX + 1).append(']');
                            if (filter.equals(VCFConstants.PASSES_FILTERS_v4) || filter.contains(VCFConstants.FILTER_CODE_SEPARATOR)) {
                                if (negated) {
                                    sb.append(" != '");
                                } else {
                                    sb.append(" = '");
                                }
                                sb.append(filter).append('\'');
                            } else {
                                if (negated) {
                                    sb.append(" NOT ");
                                }
                                sb.append(" LIKE '%").append(filter).append("%'");
                            }
                        }
                        sb.append(" ) ");
                    }
                    if (!qualValues.isEmpty()) {
                        // ( "FILE"[3] = 'N' AND ( "FILE"[5] = 'FILTER_1' OR "FILE"[5] = 'FILTER_2' ) )
                        sb.append(" AND ( ");
                        for (int i = 0; i < qualValues.size(); i++) {
                            String qualValue = qualValues.get(i);
                            String[] strings = splitOperator(qualValue);
                            String op = strings[1];
                            String qual = strings[2];

                            if (i > 0 && qualOperation != null) {
                                sb.append(' ').append(qualOperation.name()).append(' ');
                            }

                            sb.append("TO_NUMBER(");
                            sb.append('"');
                            buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                            sb.append('"');

                            // Arrays in SQL are 1-based.
                            sb.append('[').append(HBaseToStudyEntryConverter.FILE_QUAL_IDX + 1).append(']');
                            sb.append(')');

                            double parsedValue = parseDouble(qual, QUAL, qualValue);
                            sb.append(parseNumericOperator(op)).append(' ').append(parsedValue);
                        }
                        sb.append(" ) ");
                    }
                }
                sb.append(" ) ");
                if (iterator.hasNext()) {
                    if (operation == null) {
                        // This should never happen!
                        throw new VariantQueryException("Unexpected error");
                    } else if (operation.equals(QueryOperation.AND)) {
                        sb.append(" AND ");
                    } else {
                        sb.append(" OR ");
                    }
                }
            }
            filters.add(sb.toString());
        }

        if (isValidParam(query, COHORT)) {
            for (String cohort : query.getAsStringList(COHORT.key())) {
                boolean negated = false;
                if (isNegated(cohort)) {
                    cohort = removeNegation(cohort);
                    negated = true;
                }
                String[] studyCohort = splitStudyResource(cohort);
                StudyConfiguration studyConfiguration;
                if (studyCohort.length == 2) {
                    studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyCohort[0], defaultStudyConfiguration, null);
                    cohort = studyCohort[1];
                } else if (studyCohort.length == 1) {
                    studyConfiguration = defaultStudyConfiguration;
                } else {
                    throw VariantQueryException.malformedParam(COHORT, query.getString((COHORT.key())), "Expected {study}:{cohort}");
                }
                int cohortId = studyConfigurationManager.getCohortId(cohort, studyConfiguration);
                Column column = getStatsColumn(studyConfiguration.getStudyId(), cohortId);
                if (negated) {
                    filters.add("\"" + column + "\" IS NULL");
                } else {
                    filters.add("\"" + column + "\" IS NOT NULL");
                }
            }
        }

        Map<Object, List<String>> genotypesMap = new HashMap<>();
        QueryOperation genotypeQueryOperation = QueryOperation.AND;
        if (isValidParam(query, GENOTYPE)) {
            // NA12877_01 :  0/0  ;  NA12878_01 :  0/1  ,  1/1
            genotypeQueryOperation = parseGenotypeFilter(query.getString(GENOTYPE.key()), genotypesMap);
        }
        if (isValidParam(query, SAMPLE)) {
            String value = query.getString(SAMPLE.key());
            QueryOperation op = checkOperator(value);
            List<String> samples = splitValue(value, op);
            for (String sample : samples) {
                genotypesMap.put(sample, Collections.singletonList(ALT_GT));
//                genotypesMap.put(sample, Arrays.asList(NOT + HOM_REF, NOT + NOCALL, NOT + "./."));
            }
        }

        if (!genotypesMap.isEmpty()) {
            List<String> gtFilters = new ArrayList<>(genotypesMap.size());
            for (Map.Entry<Object, List<String>> entry : genotypesMap.entrySet()) {
                if (defaultStudyConfiguration == null) {
                    List<String> studyNames = studyConfigurationManager.getStudyNames(null);
                    throw VariantQueryException.missingStudyForSample(entry.getKey().toString(), studyNames);
                }
                int studyId = defaultStudyConfiguration.getStudyId();
                int sampleId = studyConfigurationManager.getSampleId(entry.getKey(), defaultStudyConfiguration);
                Set<String> genotypes = new LinkedHashSet<>(entry.getValue().size());
                List<String> loadedGenotypes;
                if (defaultStudyConfiguration.getAttributes().containsKey(HadoopVariantStorageEngine.LOADED_GENOTYPES)) {
                    loadedGenotypes = defaultStudyConfiguration.getAttributes()
                            .getAsStringList(HadoopVariantStorageEngine.LOADED_GENOTYPES);
                } else {
                    loadedGenotypes = DEFAULT_LOADED_GENOTYPES;
                }

                for (String gt : entry.getValue()) {
                    GenotypeClass genotypeClass = GenotypeClass.from(gt);
                    if (genotypeClass == null) {
                        genotypes.add(gt);
                    } else {
                        genotypes.addAll(genotypeClass.filter(loadedGenotypes));
                        genotypes.addAll(genotypeClass.filter("0/0", "./."));
                    }
                }
                // If empty, should find none. Add non-existing genotype
                // TODO: Fast empty result
                if (genotypes.isEmpty()) {
                    genotypes.add("x/x");
                }

                List<String> sampleGtFilters = new ArrayList<>(genotypes.size());
                final boolean negated;
                if (genotypes.stream().allMatch(VariantQueryUtils::isNegated)) {
                    negated = true;
                } else if (genotypes.stream().anyMatch(VariantQueryUtils::isNegated)) {
                    throw VariantQueryException.malformedParam(GENOTYPE, query.getString(GENOTYPE.key()),
                            "Can not mix negated and not negated genotypes");
                } else {
                    negated = false;
                }
                for (String genotype : genotypes) {
                    if (negated) {
                        genotype = removeNegation(genotype);
                    }
                    String key = buildSampleColumnKey(studyId, sampleId, new StringBuilder()).toString();
                    final String filter;
                    if (ALT_GT.equals(genotype)) {
                        // Either starts or ends by 1 : 0/1, 1/1, 0|1, 1|0, 1/2, ...
//                        filter = "( \"" + key + "\"[1] LIKE '%1' OR \"" + key + "\"[1] LIKE '1%' )";

                        // The genotype contains a 1: 0/1, 1/1, 0|1, 1|0, 1/2, ...
                        filter = '"' + key + "\"[1] LIKE '%1%'";
                        // FIXME: This may return unwanted values at big multi-allelic variants like : 5/10
                    } else if (FillGapsTask.isHomRefDiploid(genotype)) {
                        if (negated) {
                            filter = '"' + key + "\" IS NOT NULL AND \"" + key + "\"[1] != '" + genotype + '\'';
                        } else {
                            filter = "( \"" + key + "\"[1] = '" + genotype + "' OR \"" + key + "\" IS NULL )";
                        }
                    } else {
                        if (negated) {
                            filter = "( \"" + key + "\"[1] != '" + genotype + "' OR \"" + key + "\" IS NULL )";
                        } else {
                            filter = '"' + key + "\"[1] = '" + genotype + '\'';
                        }
                    }
                    sampleGtFilters.add(filter);
                }
                if (negated) {
                    gtFilters.add(appendFilters(sampleGtFilters, QueryOperation.AND));
                } else {
                    gtFilters.add(appendFilters(sampleGtFilters, QueryOperation.OR));
                }
            }
            filters.add(appendFilters(gtFilters, genotypeQueryOperation));
        }

        if (isValidParam(query, RELEASE)) {
            int release = query.getInt(RELEASE.key(), -1);
            if (release <= 0) {
                throw VariantQueryException.malformedParam(RELEASE, query.getString(RELEASE.key()));
            }

            StringBuilder releaseFilters = new StringBuilder();
            releaseFilters.append(buildFilter(getReleaseColumn(1), "=", true));
            for (int i = 2; i <= release; i++) {
                releaseFilters.append(" OR ").append(buildFilter(getReleaseColumn(i), "=", true));
            }
            filters.add(releaseFilters.toString());
        }

        return defaultStudyConfiguration;
    }

    private void unsupportedFilter(Query query, VariantQueryParam param) {
        if (isValidParam(query, param)) {
            String warn = "Unsupported filter \"" + param + "\"";
//            warnings.add(warn);
            logger.warn(warn);
        }
    }

    protected void addAnnotFilters(Query query, Set<Column> dynamicColumns, List<String> filters) {
        if (isValidParam(query, ANNOTATION_EXISTS)) {
            if (query.getBoolean(ANNOTATION_EXISTS.key())) {
                filters.add(VariantColumn.FULL_ANNOTATION + " IS NOT NULL");
            } else {
                filters.add(VariantColumn.FULL_ANNOTATION + " IS NULL");
            }
        }


        addQueryFilter(query, ANNOT_CONSEQUENCE_TYPE, VariantColumn.SO, filters, VariantQueryUtils::parseConsequenceType);

        addQueryFilter(query, ANNOT_BIOTYPE, VariantColumn.BIOTYPE, filters);

        addQueryFilter(query, ANNOT_SIFT, (keyOpValue, rawValue) -> {
            if (StringUtils.isNotEmpty(keyOpValue[0])) {
                throw VariantQueryException.malformedParam(ANNOT_SIFT, Arrays.toString(keyOpValue));
            }
            if (NumberUtils.isParsable(keyOpValue[2])) {
                return VariantColumn.SIFT;
            } else {
                return VariantColumn.SIFT_DESC;
            }
        }, null, null, null, filters, op -> op.contains(">") ? 2 : op.contains("<") ? 1 : -1);

        addQueryFilter(query, ANNOT_POLYPHEN, (keyOpValue, rawValue) -> {
            if (StringUtils.isNotEmpty(keyOpValue[0])) {
                throw VariantQueryException.malformedParam(ANNOT_POLYPHEN, Arrays.toString(keyOpValue));
            }
            if (NumberUtils.isParsable(keyOpValue[2])) {
                return VariantColumn.POLYPHEN;
            } else {
                return VariantColumn.POLYPHEN_DESC;
            }
        }, null, null, null, filters, op -> op.contains(">") ? 2 : op.contains("<") ? 1 : -1);

        addQueryFilter(query, ANNOT_CONSERVATION,
                (keyOpValue, rawValue) -> getConservationScoreColumn(keyOpValue[0], rawValue, true), null, filters);

        /*
         * maf < 0.3 --> PF < 0.3 OR PF >= 0.7
         * maf > 0.3 --> PF > 0.3 AND PF <= 0.7
         */
        addQueryFilter(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY,
                (keyOpValue, s) -> {
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
//                    dynamicColumns.add(column);
                    return column;
                }, null, null,
                keyOpValue -> {
                    String op = keyOpValue[1];
                    double value = Double.parseDouble(keyOpValue[2]);
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
                    if (op.startsWith("<")) {
                        // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                        return " OR \"" + column.column() + "\"[2] " + op + " " + value
                                + " OR \"" + column.column() + "\"[2] IS NULL";
                    } else if (op.startsWith(">")) {
                        return " AND \"" + column.column() + "\"[2] " + op + " " + value;
                    } else {
                        throw VariantQueryException.malformedParam(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, Arrays.toString(keyOpValue),
                                "Unable to use operator " + op + " with this query.");
                    }
                }, filters, 1);

        addQueryFilter(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY,
                (keyOpValue, s) -> {
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
//                    dynamicColumns.add(column);
                    return column;
                }, null, null,
                keyOpValue -> {
                    // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                    if (keyOpValue[1].startsWith("<")) {
                        return " OR \"" + getPopulationFrequencyColumn(keyOpValue[0]).column() + "\"[2] IS NULL";
                    }
                    return "";
                }, filters, 2);

        addQueryFilter(query, ANNOT_POPULATION_REFERENCE_FREQUENCY,
                (keyOpValue, s) -> {
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
//                    dynamicColumns.add(column);
                    return column;
                }, null, null,
                keyOpValue -> {
                    // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                    if (keyOpValue[1].startsWith("<")) {
                        return " OR \"" + getPopulationFrequencyColumn(keyOpValue[0]).column() + "\"[1] IS NULL";
                    }
                    return "";
                }, filters, 1);

        addQueryFilter(query, ANNOT_TRANSCRIPTION_FLAG, VariantColumn.TRANSCRIPTION_FLAGS, filters);

        addQueryFilter(query, ANNOT_GENE_TRAIT_ID, VariantColumn.XREFS, filters);

        addQueryFilter(query, ANNOT_GENE_TRAIT_NAME, VariantColumn.GENE_TRAITS_NAME, filters);

        addQueryFilter(query, ANNOT_HPO, VariantColumn.XREFS, filters);

        if (isValidParam(query, ANNOT_GO_GENES)) {
            String value = query.getString(ANNOT_GO_GENES.key());
            if (checkOperator(value) == QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_GO, value, "Unimplemented AND operator");
            }
            List<String> genesByGo = splitValue(value, QueryOperation.OR);
            if (genesByGo.isEmpty()) {
                // If any gene was found, the query will return no results.
                // FIXME: Find another way of returning empty results
                filters.add(getVoidFilter());
            } else {
                addQueryFilter(new Query(ANNOT_GO.key(), genesByGo), ANNOT_GO, VariantColumn.GENES, filters);
            }

        }
        if (isValidParam(query, ANNOT_EXPRESSION_GENES)) {
            String value = query.getString(ANNOT_EXPRESSION.key());
            if (checkOperator(value) == QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_EXPRESSION, value, "Unimplemented AND operator");
            }
            List<String> genesByExpression = splitValue(value, QueryOperation.OR);
            if (genesByExpression.isEmpty()) {
                // If any gene was found, the query will return no results.
                // FIXME: Find another way of returning empty results
                filters.add(getVoidFilter());
            } else {
                addQueryFilter(new Query(ANNOT_EXPRESSION.key(), genesByExpression), ANNOT_EXPRESSION, VariantColumn.GENES, filters);
            }
        }

        addQueryFilter(query, ANNOT_PROTEIN_KEYWORD, VariantColumn.PROTEIN_KEYWORDS, filters);

        addQueryFilter(query, ANNOT_DRUG, VariantColumn.DRUG, filters);

        addQueryFilter(query, ANNOT_FUNCTIONAL_SCORE,
                (keyOpValue, rawValue) -> getFunctionalScoreColumn(keyOpValue[0], rawValue), null, filters);
    }

    /**
     * @return a filter which does not match with any chromosome.
     */
    private String getVoidFilter() {
        return buildFilter(VariantColumn.CHROMOSOME, "=", "_VOID");
    }

    protected void addStatsFilters(Query query, StudyConfiguration defaultStudyConfiguration, List<String> filters) {
        addQueryFilter(query, STATS_MAF, getStatsColumnParser(defaultStudyConfiguration, VariantPhoenixHelper::getMafColumn),
                null, filters);

        addQueryFilter(query, STATS_MGF, getStatsColumnParser(defaultStudyConfiguration, VariantPhoenixHelper::getMgfColumn),
                null, filters);

        unsupportedFilter(query, MISSING_ALLELES);

        unsupportedFilter(query, MISSING_GENOTYPES);
    }

    private BiFunction<String[], String, Column> getStatsColumnParser(StudyConfiguration defaultStudyConfiguration,
                                                                      BiFunction<Integer, Integer, Column> columnBuilder) {
        return (keyOpValue, v) -> {
            String key = keyOpValue[0];
            String[] split = VariantQueryUtils.splitStudyResource(key);

            String cohort;
            final StudyConfiguration sc;
            if (split.length == 2) {
                String study = split[0];
                cohort = split[1];
                sc = studyConfigurationManager.getStudyConfiguration(study, defaultStudyConfiguration, null);
            } else {
                cohort = key;
                sc = defaultStudyConfiguration;
            }
            int cohortId = studyConfigurationManager.getCohortId(cohort, sc);

            return columnBuilder.apply(sc.getStudyId(), cohortId);
        };
    }


    private void addQueryFilter(Query query, VariantQueryParam param, Column column, List<String> filters) {
        addQueryFilter(query, param, column, filters, null);
    }

    private void addQueryFilter(Query query, VariantQueryParam param, Column column, List<String> filters,
                                Function<String, Object> valueParser) {
        addQueryFilter(query, param, (a, s) -> column, null, valueParser, null, filters);
    }

    private void addQueryFilter(Query query, VariantQueryParam param, BiFunction<String[], String, Column> columnParser,
                                Function<String, Object> valueParser, List<String> filters) {
        addQueryFilter(query, param, columnParser, null, valueParser, null, filters);
    }

    /**
     * Transforms a Key-Value from a query into a valid SQL filter.
     *
     * @param query             Query with the values
     * @param param             Param to read from the query
     * @param columnParser      Column parser. Given the [key, op, value] and the original value, returns a {@link Column}
     * @param operatorParser    Operator parser. Given the [key, op, value], returns a valid SQL operator
     * @param valueParser       Value parser. Given the [key, op, value], transforms the value to make the query.
     *                          If the returned value is a Collection, uses each value for the query.
     * @param extraFilters      Provides extra filters to be concatenated to the filter.
     * @param filters           List of filters to be modified.
     */
    private void addQueryFilter(Query query, VariantQueryParam param,
                                BiFunction<String[], String, Column> columnParser,
                                Function<String, String> operatorParser,
                                Function<String, Object> valueParser, Function<String[], String> extraFilters, List<String> filters) {
        addQueryFilter(query, param, columnParser, operatorParser, valueParser, extraFilters, filters, -1);
    }

    /**
     * Transforms a Key-Value from a query into a valid SQL filter.
     *
     * @param query             Query with the values
     * @param param             Param to read from the query
     * @param columnParser      Column parser. Given the [key, op, value] and the original value, returns a {@link Column}
     * @param operatorParser    Operator parser. Given the [key, op, value], returns a valid SQL operator
     * @param valueParser       Value parser. Given the [key, op, value], transforms the value to make the query.
     *                          If the returned value is a Collection, uses each value for the query.
     * @param extraFilters      Provides extra filters to be concatenated to the filter.
     * @param filters           List of filters to be modified.
     * @param arrayIdx          Array accessor index in base-1.
     */
    private void addQueryFilter(Query query, VariantQueryParam param,
                                BiFunction<String[], String, Column> columnParser,
                                Function<String, String> operatorParser,
                                Function<String, Object> valueParser,
                                Function<String[], String> extraFilters, List<String> filters, int arrayIdx) {
        addQueryFilter(query, param, columnParser, operatorParser, valueParser, extraFilters, filters, (o) -> arrayIdx);
    }

    /**
     * Transforms a Key-Value from a query into a valid SQL filter.
     *
     * @param query             Query with the values
     * @param param             Param to read from the query
     * @param columnParser      Column parser. Given the [key, op, value] and the original value, returns a {@link Column}
     * @param operatorParser    Operator parser. Given the [key, op, value], returns a valid SQL operator
     * @param valueParser       Value parser. Given the [key, op, value], transforms the value to make the query.
     *                          If the returned value is a Collection, uses each value for the query.
     * @param extraFilters      Provides extra filters to be concatenated to the filter.
     * @param filters           List of filters to be modified.
     * @param arrayIdxParser    Array accessor index in base-1.
     */
    private void addQueryFilter(Query query, VariantQueryParam param,
                                BiFunction<String[], String, Column> columnParser,
                                Function<String, String> operatorParser,
                                Function<String, Object> valueParser,
                                Function<String[], String> extraFilters, List<String> filters, Function<String, Integer> arrayIdxParser) {
        if (isValidParam(query, param)) {
            List<String> subFilters = new LinkedList<>();
            String stringValue = query.getString(param.key());
            QueryOperation logicOperation = checkOperator(stringValue);
            if (logicOperation == null) {
                logicOperation = QueryOperation.AND;
            }

            for (String rawValue : splitValue(stringValue, logicOperation)) {
                String[] keyOpValue = splitOperator(rawValue);
                Column column = columnParser.apply(keyOpValue, rawValue);


                String op = parseOperator(keyOpValue[1]);
                if (operatorParser != null) {
                    op = operatorParser.apply(op);
                }
                int arrayIdx = arrayIdxParser.apply(op);

                if (!column.getPDataType().isArrayType() && arrayIdx >= 0) {
                    throw new VariantQueryException("Unable to use array indexes with non array columns. "
                            + column + " " + column.sqlType());
                }

                final String negatedStr;
                boolean negated = false;
                if (op.startsWith("!")) {
                    op = inverseOperator(op);
                    negated = true;
                    negatedStr = "NOT ";
                } else {
                    negatedStr = "";
                }

                String extra = "";
                if (extraFilters != null) {
                    extra = extraFilters.apply(keyOpValue);
                }

                if (valueParser != null) {
                    Object value = valueParser.apply(keyOpValue[2]);
                    if (value instanceof Collection) {
                        List<String> subSubFilters = new ArrayList<>(((Collection) value).size());
                        for (Object o : ((Collection) value)) {
                            subSubFilters.add(buildFilter(column, op, o.toString(), "", extra, arrayIdx, param, rawValue));
                        }
                        subFilters.add(negatedStr + appendFilters(subSubFilters, QueryOperation.OR));
                    } else {
                        subFilters.add(buildFilter(column, op, value.toString(), negatedStr, extra, arrayIdx, param, rawValue));
                    }
                } else {
                    subFilters.add(buildFilter(column, op, keyOpValue[2], negatedStr, extra, arrayIdx, param, rawValue));
                }
            }
            filters.add(appendFilters(subFilters, logicOperation));
//            filters.add(subFilters.stream().collect(Collectors.joining(" ) " + operation.name() + " ( ", " ( ", " ) ")));
        }
    }

    private String buildFilter(Column column, String op, Object value) {
        return buildFilter(column, op, value, "", "", 0, null, null);
    }

    private String buildFilter(Column column, String op, Object value, boolean negated) {
        return buildFilter(column, op, value, negated ? "NOT " : "", "", 0, null, null);
    }


    private String buildFilter(Column column, String op, Object value, String negated, String extra, int idx,
                               VariantQueryParam param, String rawValue) {
        Object parsedValue;
        StringBuilder sb = new StringBuilder();

        String arrayPosition = "";

        if (StringUtils.isNotEmpty(extra)) {
            sb.append("( ");
        }
        String sqlType = column.sqlType();
        if (idx > 0) {
            sqlType = sqlType.replace(" ARRAY", "");
            arrayPosition = "[" + idx + "]";
        }
        switch (sqlType) {
            case "VARCHAR":
                parsedValue = checkStringValue((String) value);
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ');
                if (((String) parsedValue).isEmpty()) {
                    sb.append("IS NULL");
                } else {
                    sb.append(parseOperator(op))
                            .append(" '").append(parsedValue).append('\'');
                }
                break;
            case "VARCHAR ARRAY":
                parsedValue = checkStringValue((String) value);
                checkStringValue((String) parsedValue);
                sb.append(negated)
                        .append('\'').append(parsedValue).append("' ")
                        .append(parseOperator(op))
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "INTEGER ARRAY":
                parsedValue = parseInteger(value, param, rawValue);
                String operator = flipOperator(parseNumericOperator(op));
                sb.append(negated)
                        .append(parsedValue).append(' ')
                        .append(operator)
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "INTEGER":
            case "UNSIGNED_INT":
                parsedValue = parseInteger(value, param, rawValue);
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ')
                        .append(parseNumericOperator(op))
                        .append(' ').append(parsedValue);
                break;
            case "FLOAT ARRAY":
            case "DOUBLE ARRAY":
                parsedValue = parseDouble(value, param, rawValue);
                String flipOperator = flipOperator(parseNumericOperator(op));
                sb.append(negated)
                        .append(parsedValue).append(' ')
                        .append(flipOperator)
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "FLOAT":
            case "DOUBLE":
                parsedValue = parseDouble(value, param, rawValue);
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ')
                        .append(parseNumericOperator(op))
                        .append(' ').append(parsedValue);
                break;
            case "BOOLEAN":
                parsedValue = parseBoolean(value);
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ')
                        .append(parseBooleanOperator(op))
                        .append(' ').append(parsedValue);
                break;
            default:
                throw new VariantQueryException("Unsupported column type " + column.getPDataType().getSqlTypeName()
                        + " for column " + column);
        }
        if (StringUtils.isNotEmpty(extra)) {
            sb.append(' ').append(extra).append(" )");
        }
        return sb.toString();
    }

    private double parseDouble(Object value, VariantQueryParam param, String rawValue) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else {
            try {
                return Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
                if (param != null) {
                    throw VariantQueryException.malformedParam(param, rawValue);
                } else {
                    throw new VariantQueryException("Error parsing decimal value '" + value + '\'', e);
                }
            }
        }
    }

    private int parseInteger(Object value, VariantQueryParam param, String rawValue) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else {
            try {
                return Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                if (param != null) {
                    throw VariantQueryException.malformedParam(param, rawValue);
                } else {
                    throw new VariantQueryException("Error parsing integer value '" + value + '\'', e);
                }
            }
        }
    }

    private boolean parseBoolean(Object value) {
        if (value instanceof Boolean) {
            return ((Boolean) value);
        } else {
            return Boolean.parseBoolean(value.toString());
        }
    }

    private String checkStringValue(String value) {
        if (value == null) {
            throw new VariantQueryException("Unable to query null text field");
        }
        if (value.contains("'")) {
            throw new VariantQueryException("Unable to query text field with \"'\" : " + value);
        }
        return value;
    }

    /**
     * Flip the operator to flip the order of the operands.
     *
     * ">" --> "<"
     * "<" --> ">"
     * ">=" --> "<="
     * "<=" --> ">="
     *
     * @param op    Operation to flip
     * @return      Operation flipped
     */
    public static String flipOperator(String op) {
        StringBuilder sb = new StringBuilder(op.length());
        for (int i = 0; i < op.length(); i++) {
            char c = op.charAt(i);
            if (c == '>') {
                c = '<';
            } else if (c == '<') {
                c = '>';
            }
            sb.append(c);
        }
        return sb.toString();
//        return op.replace(">", "G").replace("<", ">").replace("G", "<");
    }

    /**
     * Inverse the operator obtaining the opposite operator.
     *
     * ">" --> "<="
     * "<" --> ">="
     * ">=" --> "<"
     * "<=" --> ">"
     *
     * @param op    Operation to inverse
     * @return      Operation inverted
     */
    public static String inverseOperator(String op) {
        switch (op) {
            case ">":
                return "<=";
            case ">=":
                return "<";
            case "<":
                return ">=";
            case "<=":
                return ">";
            case "":
            case "=":
            case "==":
                return "!=";
            case "!":
            case "!=":
                return "=";
            default:
                throw new VariantQueryException("Unknown operator " + op);
        }
    }

    public static String parseOperator(String op) {
        return SQL_OPERATOR.getOrDefault(op, op);
    }

    public static String parseNumericOperator(String op) {
        String parsedOp = parseOperator(op);
        if (parsedOp.equals("LIKE")) {
            throw new VariantQueryException("Unable to use REGEX operator (" + op + ") with numerical fields");
        }
        return parsedOp;
    }

    public static String parseBooleanOperator(String op) {
        String parsedOp = parseOperator(op);
        if (!parsedOp.equals("=") && !parsedOp.equals("!=")) {
            throw new VariantQueryException("Unable to use operator (" + op + ") with boolean fields");
        }
        return parsedOp;
    }


}
