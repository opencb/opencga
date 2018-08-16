package org.opencb.opencga.catalog.stats.solr;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by wasim on 09/07/18.
 */

public class CatalogSolrQueryParser {

    private static List<String> queryParameters = new ArrayList<>();
    protected static Logger logger = LoggerFactory.getLogger(CatalogSolrQueryParser.class);

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

    }

    public CatalogSolrQueryParser() {
    }

    /**
     * Create a SolrQuery object from Query and QueryOptions.
     *
     * @param query        Query
     * @param queryOptions Query Options
     * @return SolrQuery
     */
    public SolrQuery parse(Query query, QueryOptions queryOptions) {

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
                    filterList.put("studyId", query.getString(queryParam).replace(":", "_"));
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

}
