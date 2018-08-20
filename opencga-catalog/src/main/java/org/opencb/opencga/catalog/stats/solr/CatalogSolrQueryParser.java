package org.opencb.opencga.catalog.stats.solr;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.VariableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by wasim on 09/07/18.
 */

public class CatalogSolrQueryParser {

    private static List<String> queryParameters = new ArrayList<>();
    protected static Logger logger = LoggerFactory.getLogger(CatalogSolrQueryParser.class);

    public static final Pattern OPERATION_PATTERN = Pattern.compile("^(<=?|>=?|!==?|!?=?~|==?=?)([^=<>~!]+.*)$");

    static {
        // common
        queryParameters.add("study");
        queryParameters.add("type");
        queryParameters.add("status");
        queryParameters.add("creationDate");
        queryParameters.add("release");

        queryParameters.add("format");
        queryParameters.add("bioformat");
        queryParameters.add("size");
        queryParameters.add("samples");

        queryParameters.add("sex");
        queryParameters.add("ethnicity");
        queryParameters.add("population");
        queryParameters.add("source");
        queryParameters.add("somatic");
        queryParameters.add("acl");

        queryParameters.add(Constants.ANNOTATION);

    }

    public CatalogSolrQueryParser() {
    }

    /**
     * Create a SolrQuery object from Query and QueryOptions.
     *
     * @param query        Query
     * @param queryOptions Query Options
     * @param variableSetList List of variable sets available for the study whose query is going to be parsed.
     * @return SolrQuery
     */
    public SolrQuery parse(Query query, QueryOptions queryOptions, List<VariableSet> variableSetList) {

        Map<String, String> filterList = new HashMap<>();
        SolrQuery solrQuery = new SolrQuery();

        //-------------------------------------
        // Facet processing
        //-------------------------------------

        // facet fields (query parameter: facet)
        // multiple faceted fields are separated by ";", they can be:
        //    - non-nested faceted fields, e.g.: biotype
        //    - nested faceted fields (i.e., Solr pivots) are separated by ">>", e.g.: studies>>type
        //    - ranges, field_name:start:end:gap, e.g.: sift:0:1:0.5
        //    - intersections, field_name:value1^value2[^value3], e.g.: studies:1kG^ESP
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            parseSolrFacets(queryOptions.get(QueryOptions.FACET).toString(), solrQuery);
        }

        // facet ranges,
        // query parameter name: facetRange
        // multiple facet ranges are separated by ";"
        // query parameter value: field:start:end:gap, e.g.: sift:0:1:0.5
        if (queryOptions.containsKey(QueryOptions.FACET_RANGE)
                && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET_RANGE))) {
            parseSolrFacetRanges(queryOptions.get(QueryOptions.FACET_RANGE).toString(), solrQuery);
        }

        queryParameters.forEach(queryParam -> {
            if (query.containsKey(queryParam)) {
                if (queryParam.equals("study")) {
                    filterList.put("studyId", query.getString(queryParam).replace(":", "__"));
                } else if (queryParam.equals(Constants.ANNOTATION)) {
                    try {
                        filterList.putAll(parseAnnotationQueryField(query.getString(Constants.ANNOTATION),
                                query.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class), variableSetList));
                    } catch (CatalogException e) {
                        e.printStackTrace();
                    }
                } else {
                    filterList.put(queryParam, query.getString(queryParam));
                }
            }
        });

        logger.debug("query = {}\n", query.toJson());

        solrQuery.setQuery("*:*");
        // We only want stats, so we avoid retrieving the first 10 results
        solrQuery.add(CommonParams.ROWS, "0");
        filterList.forEach((queryParamter, filter) -> {
            solrQuery.addFilterQuery(queryParamter.concat(":").concat(filter));
            logger.debug("Solr fq: {}\n", filter);
        });

        return solrQuery;

    }

    /**
     * Parse facets.
     * Multiple facets are separated by semicolons (;)
     * E.g.:  chromosome[1,2,3,4,5];studies[1kg,exac]>>type[snv,indel];sift:0:1:0.2;gerp:-1:3:0.5;studies:1kG_phase3^EXAC^ESP6500
     *
     * @param strFields String containing the facet definitions
     * @param solrQuery Solr query
     */
    public void parseSolrFacets(String strFields, SolrQuery solrQuery) {
        if (StringUtils.isNotEmpty(strFields) && solrQuery != null) {
            String[] fields = strFields.split("[;]");
            for (String field : fields) {
                if (field.contains("^")) {
                    // intersections
                    parseSolrFacetIntersections(field, solrQuery);
                } else if (field.contains(":")) {
                    // ranges
                    parseSolrFacetRanges(field, solrQuery);
                } else {
                    // fields (simple or nested)
                    parseSolrFacetFields(field, solrQuery);
                }
            }
        }
    }

    /**
     * Parse Solr facet fields.
     * This format is: field_name[field_values_1,field_values_2...]:skip:limit
     *
     * @param field     String containing the facet field
     * @param solrQuery Solr query
     */
    private void parseSolrFacetFields(String field, SolrQuery solrQuery) {
        String[] splits = field.split(">>");
        if (splits.length == 1) {
            // Solr field
            //solrQuery.addFacetField(field);
            parseFacetField(field, solrQuery, false);
        } else {
            // Solr pivots (nested fields)
            StringBuilder sb = new StringBuilder();
            for (String split : splits) {
                String name = parseFacetField(split, solrQuery, true);
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(name);
            }
            solrQuery.addFacetPivotField(sb.toString());
        }
    }

    /**
     * Parse field string.
     * The expected format is: field_name[field_value_1,field_value_2,...]:skip:limit.
     *
     * @param field The string to parse
     * @retrun The field name
     */
    private String parseFacetField(String field, SolrQuery solrQuery, boolean pivot) {
        String name = "";
        String[] splits1 = field.split("[\\[\\]]");
        if (splits1.length == 1) {
            String[] splits2 = field.split(":");
            if (splits2.length >= 1) {
                name = splits2[0];
                if (!pivot) {
                    solrQuery.addFacetField(name);
                }
            }
            if (splits2.length >= 2 && StringUtils.isNotEmpty(splits2[1])) {
                solrQuery.set("f." + name + ".facet.offset", splits2[1]);
            }
            if (splits2.length >= 3 && StringUtils.isNotEmpty(splits2[2])) {
                solrQuery.set("f." + name + ".facet.limit", splits2[2]);
            }
        } else {
            // first, field name
            name = splits1[0];
            if (!pivot) {
                solrQuery.addFacetField(name);
            }

            // second, includes
            // nothing to do, if includes, the other ones will be removed later

            // third, skip and limit
            if (splits1.length >= 3) {
                String[] splits2 = splits1[2].split(":");
                if (splits2.length >= 2 && StringUtils.isNotEmpty(splits2[1])) {
                    solrQuery.set("f." + name + ".facet.offset", splits2[1]);
                }
                if (splits2.length >= 3 && StringUtils.isNotEmpty(splits2[2])) {
                    solrQuery.set("f." + name + ".facet.limit", splits2[2]);
                }
            }
        }
        return name;
    }

    /**
     * Parse Solr facet range.
     * This format is: field_name:start:end:gap, e.g.: sift:0:1:0.2
     *
     * @param range     String containing the facet range definition
     * @param solrQuery Solr query
     */
    public void parseSolrFacetRanges(String range, SolrQuery solrQuery) {
        String[] split = range.split(":");
        if (split.length != 4) {
            logger.warn("Facet range '" + range + "' malformed. The expected range format is 'name:start:end:gap'");
        } else {
            try {
                Number start, end, gap;
                if (("start").equals(split[0])) {
                    start = Integer.parseInt(split[1]);
                    end = Integer.parseInt(split[2]);
                    gap = Integer.parseInt(split[3]);
                } else {
                    start = Double.parseDouble(split[1]);
                    end = Double.parseDouble(split[2]);
                    gap = Double.parseDouble(split[3]);
                }
                // Solr ranges
                solrQuery.addNumericRangeFacet(split[0], start, end, gap);
            } catch (NumberFormatException e) {
                logger.warn("Facet range '" + range + "' malformed. Range format is 'name:start:end:gap'"
                        + " where start, end and gap values are numbers.");
            }
        }
    }

    /**
     * Parse Solr facet intersection.
     *
     * @param intersection String containing the facet intersection
     * @param solrQuery    Solr query
     */
    public void parseSolrFacetIntersections(String intersection, SolrQuery solrQuery) {
        boolean error = true;
        String[] splitA = intersection.split(":");
        if (splitA.length == 2) {
            String[] splitB = splitA[1].split("\\^");
            if (splitB.length == 2) {
                error = false;
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "}" + splitA[0] + ":" + splitB[0]);
                solrQuery.addFacetQuery("{!key=" + splitB[1] + "}" + splitA[0] + ":" + splitB[1]);
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "__" + splitB[1] + "}" + splitA[0] + ":" + splitB[0]
                        + " AND " + splitA[0] + ":" + splitB[1]);

            } else if (splitB.length == 3) {
                error = false;
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "}" + splitA[0] + ":" + splitB[0]);
                solrQuery.addFacetQuery("{!key=" + splitB[1] + "}" + splitA[0] + ":" + splitB[1]);
                solrQuery.addFacetQuery("{!key=" + splitB[2] + "}" + splitA[0] + ":" + splitB[2]);
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "__" + splitB[1] + "}" + splitA[0] + ":" + splitB[0]
                        + " AND " + splitA[0] + ":" + splitB[1]);
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "__" + splitB[2] + "}" + splitA[0] + ":" + splitB[0]
                        + " AND " + splitA[0] + ":" + splitB[2]);
                solrQuery.addFacetQuery("{!key=" + splitB[1] + "__" + splitB[2] + "}" + splitA[0] + ":" + splitB[1]
                        + " AND " + splitA[0] + ":" + splitB[2]);
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "__" + splitB[1] + "__" + splitB[2] + "}" + splitA[0]
                        + ":" + splitB[0] + " AND " + splitA[0]
                        + ":" + splitB[1] + " AND " + splitA[0] + ":" + splitB[2]);
            }
        }

        if (error) {
            logger.warn("Facet intersection '" + intersection + "' malformed. The expected intersection format"
                    + " is 'name:value1^value2[^value3]', value3 is optional");
        }
    }

    private Map<String, String> parseAnnotationQueryField(String annotations, ObjectMap variableTypeMap, List<VariableSet> variableSetList)
            throws CatalogException {

//        Map<Long, String> variableSetUidIdMap = new HashMap<>();
//        variableSetList.forEach(variableSet -> variableSetUidIdMap.put(variableSet.getUid(), variableSet.getId()));

        Map<String, String> annotationMap = new HashMap<>();

        if (StringUtils.isNotEmpty(annotations)) {
            // Annotation Filter
            final String sepAnd = ";";
            String[] annotationArray = StringUtils.split(annotations, sepAnd);

            for (String annotation : annotationArray) {
                Matcher matcher = AnnotationUtils.ANNOTATION_PATTERN.matcher(annotation);

                if (matcher.find()) {
                    // Split the annotation by key - value
                    // Remove the : at the end of the variableSet
                    String variableSet = matcher.group(1).replace(":", "");
//                    long variableSetUid = Long.valueOf(matcher.group(1).replace(":", ""));
                    String key = matcher.group(2);
                    String valueString = matcher.group(3);

//                    String variableSet = variableSetUidIdMap.get(variableSetUid);

                    if (variableTypeMap == null || variableTypeMap.isEmpty()) {
                        logger.error("Internal error: The variableTypeMap is null or empty {}", variableTypeMap);
                        throw new CatalogException("Internal error. Could not build the annotation query");
                    }
                    AnnotationUtils.Type type = variableTypeMap.get(variableSet + ":" + key, AnnotationUtils.Type.class);
                    if (type == null) {
                        logger.error("Internal error: Could not find the type of the variable {}:{}", variableSet, key);
                        throw new CatalogException("Internal error. Could not find the type of the variable " + variableSet + ":" + key);
                    }

                    annotationMap.put("annotations" + getAnnotationType(type) + variableSet + "." + key,
                            getAnnotationValues(Arrays.asList(valueString.split(",")), type));
                } else {
                    throw new CatalogDBException("Annotation " + annotation + " could not be parsed to a query.");
                }
            }
        }

        return annotationMap;
    }

    private String getAnnotationType(AnnotationUtils.Type type) throws CatalogException {
        switch (type) {
            case TEXT:
                return "__s__";
            case TEXT_ARRAY:
                return "__sm__";
            case INTEGER:
                return "__i__";
            case INTEGER_ARRAY:
                return "__im__";
            case DECIMAL:
                return "__d__";
            case DECIMAL_ARRAY:
                return "__dm__";
            case BOOLEAN:
                return "__b__";
            case BOOLEAN_ARRAY:
                return "__bm__";
            default:
                throw new CatalogException("Unexpected variable type " + type);

        }
    }

    private String getAnnotationValues(List<String> values, AnnotationUtils.Type type) throws CatalogException {
        ArrayList<String> or = new ArrayList<>(values.size());
        for (String option : values) {
            Matcher matcher = OPERATION_PATTERN.matcher(option);
            String operator;
            String filter;
            if (!matcher.find()) {
                operator = "";
                filter = option;
            } else {
                operator = matcher.group(1);
                filter = matcher.group(2);
            }
            switch (type) {
                case INTEGER:
                case INTEGER_ARRAY:
                    try {
                        int intValue = Integer.parseInt(filter);
                        or.add(addNumberOperationQueryFilter(operator, intValue, intValue - 1, intValue + 1));
                    } catch (NumberFormatException e) {
                        throw new CatalogDBException("Expected an integer value - " + e.getMessage(), e);
                    }
                    break;
                case DECIMAL:
                case DECIMAL_ARRAY:
                    try {
                        double doubleValue = Double.parseDouble(filter);
                        or.add(addNumberOperationQueryFilter(operator, doubleValue, doubleValue - 1, doubleValue + 1));
                    } catch (NumberFormatException e) {
                        throw new CatalogDBException("Expected a double value - " + e.getMessage(), e);
                    }
                    break;
                case TEXT:
                case TEXT_ARRAY:
                    or.add(addStringOperationQueryFilter(operator, filter));
                    break;
                case BOOLEAN:
                case BOOLEAN_ARRAY:
                    or.add(addBooleanOperationQueryFilter(operator, Boolean.parseBoolean(filter)));
                    break;
                default:
                    break;
            }
        }
        if (or.isEmpty()) {
            return "";
        } else if (or.size() == 1) {
            return or.get(0);
        } else {
            return "(" + StringUtils.join(or, " OR ") + ")";
        }
    }

    private String addBooleanOperationQueryFilter(String operator, boolean value) throws CatalogException {
        String query;
        switch (operator) {
            case "!=":
                query = "(" + !value + ")";
                break;
            case "":
            case "=":
            case "==":
                query = "(" + value + ")";
                break;
            default:
                throw new CatalogException("Unknown boolean query operation " + operator);
        }
        return query;
    }

    private String addStringOperationQueryFilter(String operator, String filter) throws CatalogException {
        String query;
        switch (operator) {
            case "!=":
                query = "(*:* -" + filter + ")";
                break;
            case "":
            case "=":
            case "==":
                query = "(" + filter + ")";
                break;
            default:
                throw new CatalogException("Unknown string query operation " + operator);
        }
        return query;
    }

    private String addNumberOperationQueryFilter(String operator, Number value, Number valueMinus1, Number valuePlus1)
            throws CatalogException {
        String query;
        switch (operator) {
            case "<":
                query = "[* TO " + valueMinus1 + "]";
                break;
            case "<=":
                query = "[* TO " + value + "]";
                break;
            case ">":
                query = "[" + valuePlus1 + " TO *]";
                break;
            case ">=":
                query = "[" + value + " TO *]";
                break;
            case "!=":
                query = "-[" + value + " TO " + value + "]";
                break;
            case "":
            case "=":
            case "==":
                query = "[" + value + " TO " + value + "]";
                break;
            default:
                throw new CatalogException("Unknown string query operation " + operator);
        }
        return query;
    }


}
