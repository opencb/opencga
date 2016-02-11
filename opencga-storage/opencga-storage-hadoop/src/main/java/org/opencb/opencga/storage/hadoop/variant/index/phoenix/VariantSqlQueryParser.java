package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.schema.types.PFloat;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.QueryOperation;
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
        List<String> filters = getOtherFilters(query, dynamicColumns);


        appendProjectedColumns(sb, query, options);
        appendFromStatement(sb, dynamicColumns);
        appendWhereStatement(sb, regionFilters, filters);

        if (options.getInt("limit") > 0) {
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
    public StringBuilder appendProjectedColumns(StringBuilder sb, Query query, QueryOptions options) {
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
                    sb.append(",\"").append(VariantTableStudyRow.buildColumnKey(studyId, studyColumn)).append('"');
                }
            }

            sb.append(',').append(VariantColumn.FULL_ANNOTATION);

            return sb;
        }
    }

    public void appendFromStatement(StringBuilder sb, Set<Column> dynamicColumns) {
        sb.append(" FROM \"").append(variantTable).append('"');

        if (!dynamicColumns.isEmpty()) {
            sb.append(dynamicColumns.stream()
                    .map(column -> "\"" + column.column() + "\" " + column.sqlType())
                    .collect(Collectors.joining(",", " ( ", " ) "))
            );
        }

    }

    public StringBuilder appendWhereStatement(StringBuilder sb, List<String> regionFilters, List<String> filters) {
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

    public StringBuilder appendFilters(StringBuilder sb, List<String> filters, String delimiter) {
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
    public List<String> getRegionFilters(Query query) {
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

        if (isValidParam(query, ID)) {
            logger.warn("Unsupported filter " +  ID);
        }

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
     * {@link VariantQueryParams#POLYPHEN}
     * {@link VariantQueryParams#SIFT}
     * {@link VariantQueryParams#CONSERVATION}
     * {@link VariantQueryParams#POPULATION_MINOR_ALLELE_FREQUENCY}
     * {@link VariantQueryParams#ALTERNATE_FREQUENCY}
     * {@link VariantQueryParams#REFERENCE_FREQUENCY}
     *
     * Stats filters:
     * {@link VariantQueryParams#STATS_MAF}
     * {@link VariantQueryParams#STATS_MGF}
     * {@link VariantQueryParams#MISSING_ALLELES}
     * {@link VariantQueryParams#MISSING_GENOTYPES}
     *
     * @param query Query to parse
     * @param dynamicColumns Initialized empty set to be filled with dynamic columns required by the queries
     * @return List of sql filters
     */
    public List<String> getOtherFilters(Query query, final Set<Column> dynamicColumns) {
        List<String> filters = new LinkedList<>();

        // Variant filters:
        addSimpleQueryFilter(query, REFERENCE, VariantColumn.REFERENCE, filters);

        addSimpleQueryFilter(query, ALTERNATE, VariantColumn.ALTERNATE, filters);

        if (isValidParam(query, TYPE)) {
            logger.warn("Unsupported filter " +  TYPE);
        }

        final StudyConfiguration defaultStudyConfiguration;
        if (isValidParam(query, STUDIES)) {
            List<Integer> studyIds = utils.getStudyIds(query.getAsList(STUDIES.key(), ",|;"), null);
            if (studyIds.size() == 1) {
                defaultStudyConfiguration = utils.getStudyConfigurationManager().getStudyConfiguration(studyIds.get(0), null).first();
            } else {
                defaultStudyConfiguration = null;
            }
            logger.warn("Unsupported filter " +  STUDIES);
        } else {
            defaultStudyConfiguration = null;
        }

        if (isValidParam(query, FILES)) {
            logger.warn("Unsupported filter " +  FILES);
        }

        if (isValidParam(query, COHORTS)) {
            logger.warn("Unsupported filter " +  COHORTS);
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

        // Annotation filters:
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

        if (isValidParam(query, ANNOT_XREF)) {
            logger.warn("Unsupported filter " +  ANNOT_XREF);
        }

        addSimpleQueryFilter(query, ANNOT_BIOTYPE, VariantColumn.BIOTYPE, filters);

        addSimpleQueryFilter(query, SIFT, VariantColumn.SIFT, filters);

        addSimpleQueryFilter(query, POLYPHEN, VariantColumn.POLYPHEN, filters);

        addQueryFilter(query, CONSERVATION, (keyOpValue, rawValue) -> {
            String upperCaseValue = keyOpValue[0];
            if (VariantColumn.PHASTCONS.name().equalsIgnoreCase(upperCaseValue)) {
                return VariantColumn.PHASTCONS;
            } else if (VariantColumn.PHYLOP.name().equalsIgnoreCase(upperCaseValue)) {
                return VariantColumn.PHYLOP;
            } else {
                throw VariantQueryException.malformedParam(CONSERVATION, rawValue, "Unknown conservation value.");
            }
        }, filters, null);

        if (isValidParam(query, POPULATION_MINOR_ALLELE_FREQUENCY)) {
            logger.warn("Unsupported filter " +  POPULATION_MINOR_ALLELE_FREQUENCY);
        }

        addQueryFilter(query, ALTERNATE_FREQUENCY, (keyOpValue, s) -> {
            Column column = Column.build(keyOpValue[0].toUpperCase(), PFloat.INSTANCE);
            dynamicColumns.add(column);
            return column;
        }, filters, null);

        addQueryFilter(query, REFERENCE_FREQUENCY, (keyOpValue, s) -> {
            Column column = Column.build(keyOpValue[0].toUpperCase(), PFloat.INSTANCE);
            dynamicColumns.add(column);
            return column;
        }, filters, s -> 1 - Double.parseDouble(s));

        // Stats filters:


        if (isValidParam(query, STATS_MAF)) {
            logger.warn("Unsupported filter " +  STATS_MAF);
        }

        if (isValidParam(query, STATS_MGF)) {
            logger.warn("Unsupported filter " +  STATS_MGF);
        }

        if (isValidParam(query, MISSING_ALLELES)) {
            logger.warn("Unsupported filter " +  MISSING_ALLELES);
        }

        if (isValidParam(query, MISSING_GENOTYPES)) {
            logger.warn("Unsupported filter " +  MISSING_GENOTYPES);
        }

        return filters;
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
                switch (column.sqlType()) {
                    case "VARCHAR":
                        parsedValue = parser == null ? rawValue : parser.apply(rawValue);
                        subFilters.add("\"" + column + "\" = '" + parsedValue + "'");
                        break;
                    case "VARCHAR ARRAY":
                        parsedValue = parser == null ? rawValue : parser.apply(rawValue);
                        subFilters.add("'" + parsedValue + "' = ANY(\"" + column + "\")");
                        break;
                    case "INTEGER ARRAY":
                        parsedValue = parser == null ? Integer.parseInt(keyOpValue[2]) : parser.apply(keyOpValue[2]);
                        subFilters.add(parsedValue + " " + flipOperator(parseNumericOperator(keyOpValue[1])) + " ANY(\"" + column + "\")");
                        break;
                    case "INTEGER":
                        parsedValue = parser == null ? Integer.parseInt(keyOpValue[2]) : parser.apply(keyOpValue[2]);
                        subFilters.add("\"" + column + "\" " + parseNumericOperator(keyOpValue[1]) + " " + parsedValue + " ");
                        break;
                    case "FLOAT ARRAY":
                    case "DOUBLE ARRAY":
                        parsedValue = parser == null ? Double.parseDouble(keyOpValue[2]) : parser.apply(keyOpValue[2]);
                        subFilters.add(parsedValue + " " + flipOperator(parseNumericOperator(keyOpValue[1])) + " ANY(\"" + column + "\")");
                        break;
                    case "FLOAT":
                    case "DOUBLE":
                        parsedValue = parser == null ? Double.parseDouble(keyOpValue[2]) : parser.apply(keyOpValue[2]);
                        subFilters.add("\"" + column + "\" " + parseNumericOperator(keyOpValue[1]) + " " + parsedValue + " ");
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
