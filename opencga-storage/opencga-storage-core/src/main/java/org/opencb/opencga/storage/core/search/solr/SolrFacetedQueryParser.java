package org.opencb.opencga.storage.core.search.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jtarraga on 09/03/17.
 */
public class SolrFacetedQueryParser {

    private VariantDBAdaptorUtils variantDBAdaptorUtils;
    protected static Logger logger = LoggerFactory.getLogger(SolrFacetedQueryParser.class);

    public SolrFacetedQueryParser(VariantDBAdaptorUtils variantDBAdaptorUtils) {
        this.variantDBAdaptorUtils = variantDBAdaptorUtils;
    }

    /**
     * Create a SolrQuery object from FacetedQuery.
     *
     * @param facetedQuery Faceted query
     * @return SolrQuery
     */
    public SolrQuery parse(Query facetedQuery) {
        return parse(facetedQuery, new Query(), new QueryOptions());
    }

    /**
     * Create a SolrQuery object from FacetedQuery, Query and QueryOptions.
     *
     * @param facetedQuery Faceted query
     * @param query        Query
     * @param queryOptions Query Options
     * @return SolrQuery
     */
    public SolrQuery parse(Query facetedQuery, Query query, QueryOptions queryOptions) {

        // create the solrQuery from query and query options
        SolrQuery solrQuery = new SolrQueryParser(variantDBAdaptorUtils).parse(query, queryOptions);

        // set rows to 0, we are only interested in facet information
        solrQuery.setRows(0);

        // facet fields (query parameter: facet-field)
        // nested faceted fields (i.e., Solr pivots) are separated by ":", e.g.: studies:type
        if (facetedQuery.containsKey("facet-field")) {
            String[] fields = facetedQuery.get("facet-field").toString().split("[,;]");
            for (String field: fields) {
                String[] splits = field.split(":");
                if (splits.length == 1) {
                    // Solr field
                    solrQuery.addFacetField(field);
                } else {
                    StringBuilder sb = new StringBuilder();
                    for (String split: splits) {
                        if (sb.length() > 0) {
                            sb.append(",");
                        }
                        sb.append(split);
                    }
                    // Solr pivots (nested fields)
                    solrQuery.addFacetPivotField(sb.toString());
                }
            }
        }

        // TODO: should we handle facet.query ?
        // facet query
        //if (facetedQuery.containsKey("facet.query")) {
        //    solrQuery.addFacetQuery(query.get("facet.query").toString());
        //}

        // TODO: should we handle facet.prefix ?
        //if (facetedQuery.containsKey("facet.prefix")) {
        //    solrQuery.setFacetPrefix(query.get("facet.prefix").toString());
        //}

        // facet ranges,
        // query parameter name: facet-range
        // query parameter value: field:start:end:gap, e.g.: sift:0:1:0.5
        if (facetedQuery.containsKey("facet-range")) {
            String[] ranges = facetedQuery.get("facet-range").toString().split("[,;]");
            for (String range : ranges) {
                String[] split = range.split(":");
                if (split.length != 4) {
                    logger.warn("Facet range '" + range + "' malformed. The expected range format is 'name:start:end:gap'");
                } else {
                    try {
                        Double start = Double.parseDouble(split[1]);
                        Double end = Double.parseDouble(split[2]);
                        Double gap = Double.parseDouble(split[3]);
                        // Solr ranges
                        solrQuery.addNumericRangeFacet(split[0], start, end, gap);
                    } catch (NumberFormatException e) {
                        logger.warn("Facet range '" + range + "' malformed. Range format is 'name:start:end:gap'"
                                + " where start, end and gap values are numbers.");
                    }
                }
            }
        }

        return solrQuery;
    }
}
