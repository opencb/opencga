package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.phoenix.schema.types.PFloat;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Column;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn;
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

    private static final Map<String, String> SQL_OPERATOR;

    static {
        SQL_OPERATOR = new HashMap<>();
        SQL_OPERATOR.put("==", "=");
        SQL_OPERATOR.put("=~", "LIKE");
        SQL_OPERATOR.put("~", "LIKE");
    }


    public VariantSqlQueryParser(GenomeHelper genomeHelper, String variantTable, VariantDBAdaptorUtils utils) {
        this.genomeHelper = genomeHelper;
        this.variantTable = variantTable;
        this.utils = utils;
    }

    public String parse(Query query, QueryOptions options) {

        StringBuilder sb = new StringBuilder("SELECT ");

        Set<Column> dynamicColumns = new HashSet<>();
        List<String> regionFilters = getRegionFilters(query);
        List<String> filters = getOtherFilters(query, options, dynamicColumns);


        appendProjectedColumns(sb, query, options);
        appendFromStatement(sb, dynamicColumns);
        appendWhereStatement(sb, regionFilters, filters);

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
            sb.append(" LIMIT ").append(options.getInt("limit"));
        }


        return sb.toString();
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
            List<Integer> studyIds = utils.getStudyIds(options.getAsList(RETURNED_STUDIES.key()), options);
            if (studyIds == null || studyIds.isEmpty()) {
                studyIds = utils.getStudyIds(options);
            }

            sb.append(VariantColumn.CHROMOSOME).append(',')
                    .append(VariantColumn.POSITION).append(',')
                    .append(VariantColumn.REFERENCE).append(',')
                    .append(VariantColumn.ALTERNATE);

            for (Integer studyId : studyIds) {
                for (String studyColumn : STUDY_COLUMNS) {
                    sb.append(",\"").append(buildColumnKey(studyId, studyColumn)).append('"');
                }
            }

            sb.append(',').append(VariantColumn.FULL_ANNOTATION);

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
            sb.append(" AND ");
        }

        appendFilters(sb, filters, "AND");

        return sb;
    }

    protected StringBuilder appendFilters(StringBuilder sb, List<String> filters, String delimiter) {
        delimiter = " " + delimiter + " ";
        if (!filters.isEmpty()) {
            sb.append(filters.stream().collect(Collectors.joining(delimiter, " ( ", " ) ")));
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
                regionFilters.add("( " + VariantColumn.CHROMOSOME + " = '" + region.getChromosome() + "'"
                        + " AND " + VariantColumn.POSITION + " >= " + region.getStart()
                        + " AND " + VariantColumn.POSITION + " <= " + region.getEnd() + " )");
            }
        }

        addSimpleQueryFilter(query, CHROMOSOME, VariantColumn.CHROMOSOME, regionFilters);

        unsupportedFilter(query, ID);

        if (isValidParam(query, GENE)) {
            // TODO: Ask cellbase for gene region?
            for (String gene : query.getAsStringList(GENE.key())) {
                regionFilters.add("'" + gene + "' = ANY(" + VariantColumn.GENES + ")");
            }
        }

        if (regionFilters.isEmpty()) {
            // chromosome != _METADATA
            regionFilters.add(CHROMOSOME + " != '" + genomeHelper.getMetaRowKeyString() + "'");
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
        addVariantFilters(query, options, filters);

        // Annotation filters:
        addAnnotFilters(query, dynamicColumns, filters);

        // Stats filters:
        addStatsFilters(query, filters);

        return filters;
    }

    protected void addVariantFilters(Query query, QueryOptions options, List<String> filters) {
        addSimpleQueryFilter(query, REFERENCE, VariantColumn.REFERENCE, filters);

        addSimpleQueryFilter(query, ALTERNATE, VariantColumn.ALTERNATE, filters);

        unsupportedFilter(query, TYPE);

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
            if (studyIds != null && studyIds.size() == 1) {
                defaultStudyConfiguration = utils.getStudyConfigurationManager().getStudyConfiguration(studyIds.get(0), options).first();
            } else {
                defaultStudyConfiguration = null;
            }
        }

        unsupportedFilter(query, FILES);

        unsupportedFilter(query, COHORTS);

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
    }

    private void unsupportedFilter(Query query, VariantQueryParams param) {
        if (isValidParam(query, param)) {
            logger.warn("Unsupported filter " + param);
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

        unsupportedFilter(query, ANNOT_XREF);

        addSimpleQueryFilter(query, ANNOT_BIOTYPE, VariantColumn.BIOTYPE, filters);

        addQueryFilter(query, ANNOT_SIFT, (keyOpValue, rawValue) -> {
            if (StringUtils.isNotEmpty(keyOpValue[0])) {
                throw VariantQueryException.malformedParam(ANNOT_SIFT, Arrays.toString(keyOpValue));
            }
            if (NumberUtils.isParsable(keyOpValue[2])) {
                return VariantColumn.SIFT;
            } else {
                return VariantColumn.SIFT_DESC;
            }
        }, filters, null);

        addQueryFilter(query, ANNOT_POLYPHEN, (keyOpValue, rawValue) -> {
            if (StringUtils.isNotEmpty(keyOpValue[0])) {
                throw VariantQueryException.malformedParam(ANNOT_POLYPHEN, Arrays.toString(keyOpValue));
            }
            if (NumberUtils.isParsable(keyOpValue[2])) {
                return VariantColumn.POLYPHEN;
            } else {
                return VariantColumn.POLYPHEN_DESC;
            }
        }, filters, null);

        addQueryFilter(query, ANNOT_CONSERVATION, (keyOpValue, rawValue) -> {
            String upperCaseValue = keyOpValue[0];
            if (VariantColumn.PHASTCONS.name().equalsIgnoreCase(upperCaseValue)) {
                return VariantColumn.PHASTCONS;
            } else if (VariantColumn.PHYLOP.name().equalsIgnoreCase(upperCaseValue)) {
                return VariantColumn.PHYLOP;
            } else {
                throw VariantQueryException.malformedParam(ANNOT_CONSERVATION, rawValue, "Unknown conservation value.");
            }
        }, filters, null);

        unsupportedFilter(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY);

        addQueryFilter(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY, (keyOpValue, s) -> {
            Column column = Column.build(keyOpValue[0].toUpperCase(), PFloat.INSTANCE);
            dynamicColumns.add(column);
            return column;
        }, filters, null);

        addQueryFilter(query, ANNOT_POPULATION_REFERENCE_FREQUENCY, (keyOpValue, s) -> {
            Column column = Column.build(keyOpValue[0].toUpperCase(), PFloat.INSTANCE);
            dynamicColumns.add(column);
            return column;
        }, filters, s -> 1 - Double.parseDouble(s));

        addSimpleQueryFilter(query, ANNOT_TRANSCRIPTION_FLAGS, VariantColumn.TRANSCRIPTION_FLAGS, filters);

        addSimpleQueryFilter(query, ANNOT_GENE_TRAITS_ID, VariantColumn.GENE_TRAITS_ID, filters);

        addSimpleQueryFilter(query, ANNOT_GENE_TRAITS_NAME, VariantColumn.GENE_TRAITS_NAME, filters);

        addSimpleQueryFilter(query, ANNOT_PROTEIN_KEYWORDS, VariantColumn.PROTEIN_KEYWORDS, filters);


        addSimpleQueryFilter(query, ANNOT_DRUG, VariantColumn.DRUG, filters);

        unsupportedFilter(query, ANNOT_FUNCTIONAL_SCORE);
    }

    protected void addStatsFilters(Query query, List<String> filters) {
        unsupportedFilter(query, STATS_MAF);

        unsupportedFilter(query, STATS_MGF);

        unsupportedFilter(query, MISSING_ALLELES);

        unsupportedFilter(query, MISSING_GENOTYPES);
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


    public void addSimpleQueryFilter(Query query, VariantQueryParams param, Column column, List<String> filters) {
        addQueryFilter(query, param, column, filters, null);
    }

    public void addQueryFilter(Query query, VariantQueryParams param, Column column, List<String> filters,
                               Function<String, Object> parser) {
        addQueryFilter(query, param, (a, s) -> column, filters, parser);
    }

    public void addQueryFilter(Query query, VariantQueryParams param, BiFunction<String[], String, Column> columnParser,
                               List<String> filters, Function<String, Object> parser) {
        if (isValidParam(query, param)) {
            List<String> subFilters = new LinkedList<>();
            QueryOperation operation = checkOperator(query.getString(param.key()));
            if (operation == null) {
                operation = QueryOperation.AND;
            }

            for (String rawValue : query.getAsStringList(param.key(), operation.separator())) {
                Object parsedValue;
                String[] keyOpValue = splitOperator(rawValue);
                Column column = columnParser.apply(keyOpValue, rawValue);

                final String negated;
                if (rawValue.startsWith("!")) {
                    rawValue = rawValue.substring(1);
                    negated = "NOT ";
                } else {
                    negated = "";
                }

                switch (column.sqlType()) {
                    case "VARCHAR":
                        parsedValue = parser == null ? rawValue : parser.apply(rawValue);
                        subFilters.add(negated + "\"" + column + "\" = '" + parsedValue + "'");
                        break;
                    case "VARCHAR ARRAY":
                        parsedValue = parser == null ? rawValue : parser.apply(rawValue);
                        subFilters.add(negated + "'" + parsedValue + "' = ANY(\"" + column + "\")");
                        break;
                    case "INTEGER ARRAY":
                        parsedValue = parser == null ? Integer.parseInt(keyOpValue[2]) : parser.apply(keyOpValue[2]);
                        String operator = flipOperator(parseNumericOperator(keyOpValue[1]));
                        subFilters.add(negated + parsedValue + " " + operator + " ANY(\"" + column + "\")");
                        break;
                    case "INTEGER":
                        parsedValue = parser == null ? Integer.parseInt(keyOpValue[2]) : parser.apply(keyOpValue[2]);
                        subFilters.add(negated + "\"" + column + "\" " + parseNumericOperator(keyOpValue[1]) + " " + parsedValue + " ");
                        break;
                    case "FLOAT ARRAY":
                    case "DOUBLE ARRAY":
                        parsedValue = parser == null ? Double.parseDouble(keyOpValue[2]) : parser.apply(keyOpValue[2]);
                        String flipOperator = flipOperator(parseNumericOperator(keyOpValue[1]));
                        subFilters.add(negated + parsedValue + " " + flipOperator + " ANY(\"" + column + "\")");
                        break;
                    case "FLOAT":
                    case "DOUBLE":
                        parsedValue = parser == null ? Double.parseDouble(keyOpValue[2]) : parser.apply(keyOpValue[2]);
                        subFilters.add(negated + "\"" + column + "\" " + parseNumericOperator(keyOpValue[1]) + " " + parsedValue + " ");
                        break;
                    default:
                        logger.warn("Unsupported column type " + column.getPDataType().getSqlTypeName() + " for column " + column);
                        break;

                }
            }
            filters.add(subFilters.stream().collect(Collectors.joining(" " + operation.name() + " ", " ( ", " ) ")));
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
