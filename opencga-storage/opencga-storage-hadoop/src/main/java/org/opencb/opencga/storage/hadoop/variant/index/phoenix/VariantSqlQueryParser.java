package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.*;

/**
 * Created on 16/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSqlQueryParser {

    public static final String COUNT = "count";
    private final GenomeHelper genomeHelper;
    private final String variantTable;
    private final Logger logger = LoggerFactory.getLogger(VariantSqlQueryParser.class);
    private final VariantDBAdaptorUtils utils;
    private final CellBaseClient cellBaseClient;

    private static final Map<String, String> SQL_OPERATOR;

    static {
        SQL_OPERATOR = new HashMap<>();
        SQL_OPERATOR.put("==", "=");
        SQL_OPERATOR.put("=~", "LIKE");
        SQL_OPERATOR.put("~", "LIKE");
        SQL_OPERATOR.put("!", "!=");
    }


    public VariantSqlQueryParser(GenomeHelper genomeHelper, String variantTable, VariantDBAdaptorUtils utils,
                                 CellBaseClient cellBaseClient) {
        this.genomeHelper = genomeHelper;
        this.variantTable = variantTable;
        this.utils = utils;
        this.cellBaseClient = cellBaseClient;
    }

    public String parse(Query query, QueryOptions options) {

        StringBuilder sb = new StringBuilder("SELECT ");

        try {

            Set<Column> dynamicColumns = new HashSet<>();
            List<String> regionFilters = getRegionFilters(query);
            List<String> filters = getOtherFilters(query, options, dynamicColumns);

            appendProjectedColumns(sb, query, options);
            appendFromStatement(sb, dynamicColumns);
            appendWhereStatement(sb, regionFilters, filters);

        } catch (VariantQueryException e) {
            e.setQuery(query);
            throw e;
        }

        if (options.getBoolean(QueryOptions.SORT)) {
            sb.append(" ORDER BY ").append(VariantColumn.CHROMOSOME.column()).append(",").append(VariantColumn.POSITION.column());

            String order = options.getString(QueryOptions.ORDER, QueryOptions.ASCENDING);
            if (order.equalsIgnoreCase(QueryOptions.ASCENDING) || order.equalsIgnoreCase("ASC")) {
                sb.append(" ASC ");
            } else {
                sb.append(" DESC ");
            }
        }

        if (options.getInt(QueryOptions.LIMIT) > 0) {
            sb.append(" LIMIT ").append(options.getInt(QueryOptions.LIMIT));
        }


        return sb.toString();
    }

    public VariantDBAdaptorUtils getUtils() {
        return utils;
    }

    /**
     * Select only the required columns.
     *
     * Uses the params:
     * {@link VariantQueryParams#RETURNED_STUDIES}
     * {@link VariantQueryParams#RETURNED_SAMPLES}
     * {@link VariantQueryParams#RETURNED_FILES}
     * {@link VariantQueryParams#UNKNOWN_GENOTYPE}
     *
     * @param sb    SQLStringBuilder
     * @param query Query to parse
     * @param options   other options
     * @return String builder
     */
    protected StringBuilder appendProjectedColumns(StringBuilder sb, Query query, QueryOptions options) {
        if (options.getBoolean(COUNT)) {
            return sb.append(" COUNT(*) ");
        } else {

            Set<String> returnedFields = utils.getReturnedFields(options);

            List<Integer> studyIds = utils.getStudyIds(options.getAsList(RETURNED_STUDIES.key()), options);
            if (studyIds == null || studyIds.isEmpty()) {
                studyIds = utils.getStudyIds(options);
            }

            sb.append(VariantColumn.CHROMOSOME).append(',')
                    .append(VariantColumn.POSITION).append(',')
                    .append(VariantColumn.REFERENCE).append(',')
                    .append(VariantColumn.ALTERNATE).append(',')
                    .append(VariantColumn.TYPE);

            if (returnedFields.contains(STUDIES_FIELD)) {
                for (Integer studyId : studyIds) {
                    List<String> studyColumns = STUDY_COLUMNS;
//                    if (returnedFields.contains(SAMPLES_FIELD)) {
//                        studyColumns = STUDY_COLUMNS;
//                    } else {
//                        // If samples are not required, do not fetch all the fields
//                        studyColumns = Collections.singletonList(HOM_REF);
//                    }
                    for (String studyColumn : studyColumns) {
                        sb.append(",\"").append(buildColumnKey(studyId, studyColumn)).append('"');
                    }
                    if (returnedFields.contains(STATS_FIELD)) {
                        StudyConfiguration studyConfiguration = utils.getStudyConfigurationManager()
                                .getStudyConfiguration(studyId, null).first();
                        for (Integer cohortId : studyConfiguration.getCalculatedStats()) {
                            Column statsColumn = getStatsColumn(studyId, cohortId);
                            sb.append(",\"").append(statsColumn.column()).append('"');
                        }
                    }
                }
            }

            if (returnedFields.contains(ANNOTATION_FIELD)) {
                sb.append(',').append(VariantColumn.FULL_ANNOTATION);
            }

            return sb;
        }
    }

    protected void appendFromStatement(StringBuilder sb, Set<Column> dynamicColumns) {
        sb.append(" FROM \"").append(variantTable).append('"');

        if (!dynamicColumns.isEmpty()) {
            sb.append(dynamicColumns.stream()
                    .map(column -> "\"" + column.column() + "\" " + column.sqlType())
                    .collect(Collectors.joining(",", " ( ", " ) "))
            );
        }

    }

    protected StringBuilder appendWhereStatement(StringBuilder sb, List<String> regionFilters, List<String> filters) {
        if (!regionFilters.isEmpty() || !filters.isEmpty()) {
            sb.append(" WHERE");
        }

        appendFilters(sb, regionFilters, "OR");

        if (!filters.isEmpty() && !regionFilters.isEmpty()) {
            sb.append(" AND");
        }

        appendFilters(sb, filters, "AND");

        return sb;
    }

    protected String appendFilters(List<String> filters, String delimiter) {
        return appendFilters(new StringBuilder(), filters, delimiter).toString();
    }

    protected StringBuilder appendFilters(StringBuilder sb, List<String> filters, String delimiter) {
        delimiter = " " + delimiter + " ";
        if (!filters.isEmpty()) {
            sb.append(filters.stream().collect(Collectors.joining(delimiter, " ( ", " )")));
        }
        return sb;
    }

    /**
     * Transform QueryParams that are inclusive.

     * A variant will pass this filters if matches with ANY of this filters.
     *
     * {@link VariantQueryParams#REGION}
     * {@link VariantQueryParams#CHROMOSOME}
     *
     * Using annotation:
     * {@link VariantQueryParams#ID}
     * {@link VariantQueryParams#GENE}
     *
     * @param query Query to parse
     * @return List of region filters
     */
    protected List<String> getRegionFilters(Query query) {
        List<String> regionFilters = new LinkedList<>();


        if (isValidParam(query, REGION)) {
            List<Region> regions = Region.parseRegions(query.getString(REGION.key()));
            for (Region region : regions) {
                List<String> subFilters = new ArrayList<>(3);
                subFilters.add(buildFilter(VariantColumn.CHROMOSOME, "=", region.getChromosome()));
                subFilters.add(buildFilter(VariantColumn.POSITION, ">=", Integer.toString(region.getStart())));
                subFilters.add(buildFilter(VariantColumn.POSITION, "<=", Integer.toString(region.getEnd())));
                regionFilters.add(appendFilters(subFilters, QueryOperation.AND.toString()));
            }
        }

        addQueryFilter(query, CHROMOSOME, VariantColumn.CHROMOSOME, regionFilters);

//        addQueryFilter(query, ID, VariantColumn.XREFS, regionFilters);
        if (isValidParam(query, ID)) {
            for (String id : query.getAsStringList(ID.key())) {
                Variant variant = null;
                if (id.contains(":")) {
                    try {
                        variant = new Variant(id);
                    } catch (IllegalArgumentException ignore) {
                        logger.info("Wrong variant " + id);
                    }
                }
                if (variant == null) {
                    regionFilters.add(buildFilter(VariantColumn.XREFS, "=", id));
                } else {
                    List<String> subFilters = new ArrayList<>(4);
                    subFilters.add(buildFilter(VariantColumn.CHROMOSOME, "=", variant.getChromosome()));
                    subFilters.add(buildFilter(VariantColumn.POSITION, "=", variant.getStart().toString()));
                    subFilters.add(buildFilter(VariantColumn.REFERENCE, "=", variant.getReference()));
                    subFilters.add(buildFilter(VariantColumn.ALTERNATE, "=", variant.getAlternate()));
                    regionFilters.add(appendFilters(subFilters, QueryOperation.AND.toString()));
                }
            }
        }

        // TODO: Ask cellbase for gene region?
        addQueryFilter(query, GENE, VariantColumn.GENES, regionFilters);

        if (regionFilters.isEmpty()) {
            // chromosome != _METADATA
            regionFilters.add(VariantColumn.CHROMOSOME + " != '" + genomeHelper.getMetaRowKeyString() + "'");
        }
        return regionFilters;
    }

    /**
     * Transform QueryParams that are exclusive.
     *
     * A variant will pass this filters if matches with ALL of this filters.
     *
     * Variant filters:
     * {@link VariantQueryParams#REFERENCE}
     * {@link VariantQueryParams#ALTERNATE}
     * {@link VariantQueryParams#TYPE}
     * {@link VariantQueryParams#STUDIES}
     * {@link VariantQueryParams#FILES}
     * {@link VariantQueryParams#COHORTS}
     * {@link VariantQueryParams#GENOTYPE}
     *
     * Annotation filters:
     * {@link VariantQueryParams#ANNOTATION_EXISTS}
     * {@link VariantQueryParams#ANNOT_CONSEQUENCE_TYPE}
     * {@link VariantQueryParams#ANNOT_XREF}
     * {@link VariantQueryParams#ANNOT_BIOTYPE}
     * {@link VariantQueryParams#ANNOT_POLYPHEN}
     * {@link VariantQueryParams#ANNOT_SIFT}
     * {@link VariantQueryParams#ANNOT_CONSERVATION}
     * {@link VariantQueryParams#ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY}
     * {@link VariantQueryParams#ANNOT_POPULATION_ALTERNATE_FREQUENCY}
     * {@link VariantQueryParams#ANNOT_POPULATION_REFERENCE_FREQUENCY}

     * {@link VariantQueryParams#ANNOT_TRANSCRIPTION_FLAGS}
     * {@link VariantQueryParams#ANNOT_GENE_TRAITS_ID}
     * {@link VariantQueryParams#ANNOT_GENE_TRAITS_NAME}
     * {@link VariantQueryParams#ANNOT_HPO}
     * {@link VariantQueryParams#ANNOT_GO}
     * {@link VariantQueryParams#ANNOT_EXPRESSION}
     * {@link VariantQueryParams#ANNOT_PROTEIN_KEYWORDS}
     * {@link VariantQueryParams#ANNOT_DRUG}
     * {@link VariantQueryParams#ANNOT_FUNCTIONAL_SCORE}
     *
     * Stats filters:
     * {@link VariantQueryParams#STATS_MAF}
     * {@link VariantQueryParams#STATS_MGF}
     * {@link VariantQueryParams#MISSING_ALLELES}
     * {@link VariantQueryParams#MISSING_GENOTYPES}
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
        if (isValidParam(query, STUDIES)) {
            String value = query.getString(STUDIES.key());
            QueryOperation operation = checkOperator(value);
            List<String> values = splitValue(value, operation);
            StringBuilder sb = new StringBuilder();
            Iterator<String> iterator = values.iterator();
            while (iterator.hasNext()) {
                String study = iterator.next();
                Integer studyId = utils.getStudyId(study, options, false);
                if (study.startsWith("!")) {
                    sb.append("\"").append(buildColumnKey(studyId, VariantTableStudyRow.HOM_REF)).append("\" IS NULL ");
                } else {
                    sb.append("\"").append(buildColumnKey(studyId, VariantTableStudyRow.HOM_REF)).append("\" IS NOT NULL ");
                }
                if (iterator.hasNext()) {
                    if (operation == null || operation.equals(QueryOperation.AND)) {
                        sb.append(" AND ");
                    } else {
                        sb.append(" OR ");
                    }
                }
                filters.add(sb.toString());
            }
            List<Integer> studyIds = utils.getStudyIds(values, options);
            if (studyIds.size() == 1) {
                defaultStudyConfiguration = utils.getStudyConfigurationManager().getStudyConfiguration(studyIds.get(0), options).first();
            } else {
                defaultStudyConfiguration = null;
            }
        } else {
            List<Integer> studyIds = utils.getStudyConfigurationManager().getStudyIds(options);
            if (studyIds.size() == 1) {
                defaultStudyConfiguration = utils.getStudyConfigurationManager().getStudyConfiguration(studyIds.get(0), options).first();
            } else {
                defaultStudyConfiguration = null;
            }
            StringBuilder sb = new StringBuilder();
            for (Iterator<Integer> iterator = studyIds.iterator(); iterator.hasNext();) {
                Integer studyId = iterator.next();
                sb.append('"').append(buildColumnKey(studyId, HOM_REF)).append("\" IS NOT NULL");
                if (iterator.hasNext()) {
                    sb.append(" OR ");
                }
            }
            filters.add(sb.toString());
        }

        unsupportedFilter(query, FILES);

        if (isValidParam(query, COHORTS)) {
            for (String cohort : query.getAsStringList(COHORTS.key())) {
                boolean negated = false;
                if (cohort.startsWith("!")) {
                    cohort = cohort.substring(1);
                    negated = true;
                }
                String[] studyCohort = cohort.split(":");
                StudyConfiguration studyConfiguration;
                if (studyCohort.length == 2) {
                    studyConfiguration = utils.getStudyConfiguration(studyCohort[0], defaultStudyConfiguration);
                    cohort = studyCohort[1];
                } else if (studyCohort.length == 1) {
                    studyConfiguration = defaultStudyConfiguration;
                } else {
                    throw VariantQueryException.malformedParam(COHORTS, query.getString((COHORTS.key())), "Expected {study}:{cohort}");
                }
                int cohortId = utils.getCohortId(cohort, studyConfiguration);
                Column column = VariantPhoenixHelper.getStatsColumn(studyConfiguration.getStudyId(), cohortId);
                if (negated) {
                    filters.add(column + " IS NULL");
                } else {
                    filters.add(column + " IS NOT NULL");
                }
            }
        }

        //
        //
        // NA12877_01 :  0/0  ;  NA12878_01 :  0/1  ,  1/1
        if (isValidParam(query, GENOTYPE)) {
            for (String sampleGenotype : query.getAsStringList(GENOTYPE.key(), ";")) {
                //[<study>:]<sample>:<genotype>[,<genotype>]*
                String[] split = sampleGenotype.split(":");
                final List<String> genotypes;
                int studyId;
                int sampleId;
                if (split.length == 2) {
                    if (defaultStudyConfiguration == null) {
                        List<String> studyNames = utils.getStudyConfigurationManager().getStudyNames(null);
                        throw VariantQueryException.missingStudyForSample(split[0], studyNames);
                    }
                    studyId = defaultStudyConfiguration.getStudyId();
                    sampleId = utils.getSampleId(split[0], defaultStudyConfiguration);
                    genotypes = Arrays.asList(split[1].split(","));
                } else if (split.length == 3) {
                    studyId = utils.getStudyId(split[0], null, false);
                    sampleId = utils.getSampleId(split[1], defaultStudyConfiguration);
                    genotypes = Arrays.asList(split[2].split(","));
                } else {
                    throw VariantQueryException.malformedParam(GENOTYPE, sampleGenotype);
                }

                List<String> gts = new ArrayList<>(genotypes.size());
                for (String genotype : genotypes) {
                    boolean negated = false;
                    if (genotype.startsWith("!")) {
                        genotype = genotype.substring(1);
                        negated = true;
                    }
                    switch (genotype) {
                        case HET_REF:
                        case HOM_VAR:
                        case NOCALL:
//                        0 = any("1_.")
                            gts.add((negated ? " NOT " : " ") + sampleId + " = ANY(\"" + buildColumnKey(studyId, genotype) + "\") ");
                            break;
                        case HOM_REF:
                            List<String> subFilters = new ArrayList<>(4);
                            if (negated) {
                                gts.add(" ( " + sampleId + " = ANY(\"" + buildColumnKey(studyId, HET_REF) + "\") "
                                        + " OR " + sampleId + " = ANY(\"" + buildColumnKey(studyId, HOM_VAR) + "\") "
                                        + " OR " + sampleId + " = ANY(\"" + buildColumnKey(studyId, NOCALL) + "\") "
                                        + " OR " + sampleId + " = ANY(\"" + buildColumnKey(studyId, OTHER) + "\") "
                                        + " ) "
                                );
                            } else {
                                gts.add(" NOT " + sampleId + " = ANY(\"" + buildColumnKey(studyId, HET_REF) + "\") "
                                        + "AND NOT " + sampleId + " = ANY(\"" + buildColumnKey(studyId, HOM_VAR) + "\") "
                                        + "AND NOT " + sampleId + " = ANY(\"" + buildColumnKey(studyId, NOCALL) + "\") "
                                        + "AND NOT " + sampleId + " = ANY(\"" + buildColumnKey(studyId, OTHER) + "\") "
                                );
                            }
                            break;
                        default:  //OTHER
                            gts.add((negated ? " NOT " : " ") + sampleId + " = ANY(\"" + buildColumnKey(studyId, OTHER) + "\") ");
                            break;
                    }
                }
                filters.add(gts.stream().collect(Collectors.joining(" OR ", " ( ", " ) ")));
            }
        }

        return defaultStudyConfiguration;
    }

    private void unsupportedFilter(Query query, VariantQueryParams param) {
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


        addQueryFilter(query, ANNOT_CONSEQUENCE_TYPE, VariantColumn.SO, filters, so -> {
            int soAccession;
            if (so.startsWith("SO:") || StringUtils.isNumeric(so)) {
                try {
                    soAccession = Integer.parseInt(so.toUpperCase().replace("SO:", ""));
                } catch (NumberFormatException e) {
                    throw new VariantQueryException("Invalid SOAccession number", e);
                }
            } else {
                soAccession = ConsequenceTypeMappings.termToAccession.get(so);
            }
            return soAccession;
        });

        addQueryFilter(query, ANNOT_XREF, VariantColumn.XREFS, filters);

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
        }, null, filters);

        addQueryFilter(query, ANNOT_POLYPHEN, (keyOpValue, rawValue) -> {
            if (StringUtils.isNotEmpty(keyOpValue[0])) {
                throw VariantQueryException.malformedParam(ANNOT_POLYPHEN, Arrays.toString(keyOpValue));
            }
            if (NumberUtils.isParsable(keyOpValue[2])) {
                return VariantColumn.POLYPHEN;
            } else {
                return VariantColumn.POLYPHEN_DESC;
            }
        }, null, filters);

        addQueryFilter(query, ANNOT_CONSERVATION,
                (keyOpValue, rawValue) -> getConservationScoreColumn(keyOpValue[0], rawValue, true), null, filters);

        /*
         * maf < 0.3 --> PF < 0.3 OR PF >= 0.7
         * maf > 0.3 --> PF > 0.3 AND PF <= 0.7
         */
        addQueryFilter(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY,
                (keyOpValue, s) -> {
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
                    dynamicColumns.add(column);
                    return column;
                }, null, null,
                keyOpValue -> {
                    String op = keyOpValue[1];
                    double value = Double.parseDouble(keyOpValue[2]);
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
                    if (op.startsWith("<")) {
                        // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                        return " OR \"" + column.column() + "\"[2] " + op + " " + value
                                + " OR \"" + column.column() + "\" IS NULL";
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
                    dynamicColumns.add(column);
                    return column;
                }, null, null,
                keyOpValue -> {
                    // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                    if (keyOpValue[1].startsWith("<")) {
                        return " OR \"" + getPopulationFrequencyColumn(keyOpValue[0]).column() + "\" IS NULL";
                    }
                    return "";
                }, filters, 2);

        addQueryFilter(query, ANNOT_POPULATION_REFERENCE_FREQUENCY,
                (keyOpValue, s) -> {
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
                    dynamicColumns.add(column);
                    return column;
                }, null, null,
                keyOpValue -> {
                    // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                    if (keyOpValue[1].startsWith("<")) {
                        return " OR \"" + getPopulationFrequencyColumn(keyOpValue[0]).column() + "\" IS NULL";
                    }
                    return "";
                }, filters, 1);

        addQueryFilter(query, ANNOT_TRANSCRIPTION_FLAGS, VariantColumn.TRANSCRIPTION_FLAGS, filters);

        addQueryFilter(query, ANNOT_GENE_TRAITS_ID, VariantColumn.GENE_TRAITS_ID, filters);

        addQueryFilter(query, ANNOT_GENE_TRAITS_NAME, VariantColumn.GENE_TRAITS_NAME, filters);

        addQueryFilter(query, ANNOT_HPO, VariantColumn.HPO, filters);

        if (isValidParam(query, ANNOT_GO)) {
            String value = query.getString(ANNOT_GO.key());
            if (checkOperator(value) == QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParams.ANNOT_GO, value, "Unimplemented AND operator");
            }
            List<String> goValues = splitValue(value, QueryOperation.OR);
            Set<String> genesByGo = utils.getGenesByGo(goValues);
            if (genesByGo.isEmpty()) {
                // If any gene was found, the query will return no results.
                // FIXME: Find another way of returning empty results
                filters.add(buildFilter(VariantColumn.CHROMOSOME, "=", "_SKIP"));
            } else {
                addQueryFilter(new Query(ANNOT_GO.key(), genesByGo), ANNOT_GO, VariantColumn.GENES, filters);
            }

        }
        if (isValidParam(query, ANNOT_EXPRESSION)) {
            String value = query.getString(ANNOT_EXPRESSION.key());
            if (checkOperator(value) == QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParams.ANNOT_EXPRESSION, value, "Unimplemented AND operator");
            }
            List<String> expressionValues = splitValue(value, QueryOperation.OR);
            Set<String> genesByExpression = utils.getGenesByExpression(expressionValues);
            if (genesByExpression.isEmpty()) {
                // If any gene was found, the query will return no results.
                // FIXME: Find another way of returning empty results
                filters.add(buildFilter(VariantColumn.CHROMOSOME, "=", "_SKIP"));
            } else {
                addQueryFilter(new Query(ANNOT_EXPRESSION.key(), genesByExpression), ANNOT_EXPRESSION, VariantColumn.GENES, filters);
            }
        }

        addQueryFilter(query, ANNOT_PROTEIN_KEYWORDS, VariantColumn.PROTEIN_KEYWORDS, filters);

        addQueryFilter(query, ANNOT_DRUG, VariantColumn.DRUG, filters);

        addQueryFilter(query, ANNOT_FUNCTIONAL_SCORE, (keyOpValue, rawValue) -> {
            Column column = getFunctionalScoreColumn(keyOpValue[0]);
            dynamicColumns.add(column);
            return column;
        }, null, filters);
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
            int indexOf = key.lastIndexOf(":");

            String cohort;
            final StudyConfiguration sc;
            if (indexOf > 0) {
                String study = key.substring(0, indexOf);
                cohort = key.substring(indexOf + 1);
                sc = utils.getStudyConfiguration(study, defaultStudyConfiguration);
            } else {
                cohort = key;
                sc = defaultStudyConfiguration;
            }
            int cohortId = utils.getCohortId(cohort, sc);

            return columnBuilder.apply(sc.getStudyId(), cohortId);
        };
    }

    /**
     * Check if the object query contains the value param, is not null and, if is an string or a list, is not empty.
     *
     * isValidParam(new Query(), PARAM) == false
     * isValidParam(new Query(PARAM.key(), null), PARAM) == false
     * isValidParam(new Query(PARAM.key(), ""), PARAM) == false
     * isValidParam(new Query(PARAM.key(), Collections.emptyList()), PARAM) == false
     * isValidParam(new Query(PARAM.key(), 5), PARAM) == true
     * isValidParam(new Query(PARAM.key(), "sdfas"), PARAM) == true
     *
     * @param query Query to parse
     * @param param QueryParam to check
     * @return If is valid or not
     */
    public static boolean isValidParam(Query query, VariantQueryParams param) {
        Object value = query.getOrDefault(param.key(), null);
        return (value != null)
                && !(value instanceof String && ((String) value).isEmpty()
                || value instanceof Collection && ((Collection) value).isEmpty());
    }


    private void addQueryFilter(Query query, VariantQueryParams param, Column column, List<String> filters) {
        addQueryFilter(query, param, column, filters, null);
    }

    private void addQueryFilter(Query query, VariantQueryParams param, Column column, List<String> filters,
                                Function<String, Object> valueParser) {
        addQueryFilter(query, param, (a, s) -> column, null, valueParser, null, filters);
    }

    private void addQueryFilter(Query query, VariantQueryParams param, BiFunction<String[], String, Column> columnParser,
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
    private void addQueryFilter(Query query, VariantQueryParams param,
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
    private void addQueryFilter(Query query, VariantQueryParams param,
                                BiFunction<String[], String, Column> columnParser,
                                Function<String, String> operatorParser,
                                Function<String, Object> valueParser,
                                Function<String[], String> extraFilters, List<String> filters, int arrayIdx) {
        if (isValidParam(query, param)) {
            List<String> subFilters = new LinkedList<>();
            QueryOperation logicOperation = checkOperator(query.getString(param.key()));
            if (logicOperation == null) {
                logicOperation = QueryOperation.AND;
            }

            for (String rawValue : query.getAsStringList(param.key(), logicOperation.separator())) {
                String[] keyOpValue = splitOperator(rawValue);
                Column column = columnParser.apply(keyOpValue, rawValue);
                if (!column.getPDataType().isArrayType() && arrayIdx >= 0) {
                    throw new VariantQueryException("Unable to use array indexes with non array columns. "
                            + column + " " + column.sqlType());
                }

                String op = parseOperator(keyOpValue[1]);
                if (operatorParser != null) {
                    op = operatorParser.apply(op);
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
                            subSubFilters.add(buildFilter(column, op, o.toString(), "", extra, arrayIdx));
                        }
                        subFilters.add(negatedStr + appendFilters(subSubFilters, QueryOperation.OR.toString()));
                    } else {
                        subFilters.add(buildFilter(column, op, value.toString(), negatedStr, extra, arrayIdx));
                    }
                } else {
                    subFilters.add(buildFilter(column, op, keyOpValue[2], negatedStr, extra, arrayIdx));
                }
            }
            filters.add(appendFilters(subFilters, logicOperation.toString()));
//            filters.add(subFilters.stream().collect(Collectors.joining(" ) " + operation.name() + " ( ", " ( ", " ) ")));
        }
    }

    private String buildFilter(Column column, String op, String value) {
        return buildFilter(column, op, value, "", "", 0);
    }

    private String buildFilter(Column column, String op, String value, boolean negated) {
        return buildFilter(column, op, value, negated ? "NOT " : "", "", 0);
    }


    private String buildFilter(Column column, String op, Object value,
                               String negated, String extra, int idx) {
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
                parsedValue = value;
                checkStringValue((String) parsedValue);
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
                parsedValue = value;
                checkStringValue((String) parsedValue);
                sb.append(negated)
                        .append("'").append(parsedValue).append("' ")
                        .append(parseOperator(op))
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "INTEGER ARRAY":
                parsedValue = value instanceof Number ? ((Number) value).intValue() : Integer.parseInt(value.toString());
                String operator = flipOperator(parseNumericOperator(op));
                sb.append(negated)
                        .append(parsedValue).append(' ')
                        .append(operator)
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "INTEGER":
            case "UNSIGNED_INT":
                parsedValue = value instanceof Number ? ((Number) value).intValue() : Integer.parseInt(value.toString());
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ')
                        .append(parseNumericOperator(op))
                        .append(' ').append(parsedValue);
                break;
            case "FLOAT ARRAY":
            case "DOUBLE ARRAY":
                parsedValue = value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
                String flipOperator = flipOperator(parseNumericOperator(op));
                sb.append(negated)
                        .append(parsedValue).append(' ')
                        .append(flipOperator)
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "FLOAT":
            case "DOUBLE":
                parsedValue = value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ')
                        .append(parseNumericOperator(op))
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

    private void checkStringValue(String parsedValue) {
        if (parsedValue.contains("'")) {
            throw new VariantQueryException("Unable to query text field using \"'\"");
        }
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

}
