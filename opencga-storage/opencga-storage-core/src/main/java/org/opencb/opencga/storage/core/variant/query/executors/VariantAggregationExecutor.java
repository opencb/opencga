package org.opencb.opencga.storage.core.variant.query.executors;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser.CHROM_DENSITY;

/**
 * Created on 10/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantAggregationExecutor {

    protected static final Pattern CHROM_DENSITY_PATTERN = Pattern.compile("^" + CHROM_DENSITY + "\\[([a-zA-Z0-9:\\-,*]+)](:(\\d+))?$");
    protected static final String NESTED_FACET_SEPARATOR = ">>"; // FacetQueryParser.NESTED_FACET_SEPARATOR
    protected static final String FACET_SEPARATOR = ";";
    private Logger logger = LoggerFactory.getLogger(VariantAggregationExecutor.class);

    public final boolean canUseThisExecutor(Query query, QueryOptions options) {
        return canUseThisExecutor(query, options, new LinkedList<>());
    }

    public final boolean canUseThisExecutor(Query query, QueryOptions options, List<String> reason) {
        if (query == null) {
            query = new Query();
        }
        if (options == null) {
            options = new QueryOptions();
        }

        String facet = options.getString(QueryOptions.FACET);

        try {
            return canUseThisExecutor(query, options, facet, reason);
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }
    }

    /**
     * Fetch facet (i.e., counts) resulting of executing the query in the database.
     *
     * @param query          Query to be executed in the database to filter variants
     * @param options        Query modifiers, accepted values are: facet fields and facet ranges
     * @return               A FacetedQueryResult with the result of the query
     */
    public final VariantQueryResult<FacetField> aggregation(Query query, QueryOptions options) {
        if (query == null) {
            query = new Query();
        } else {
            query = new Query(query);
        }
        if (options == null) {
            options = new QueryOptions();
        } else {
            options = new QueryOptions(options);
        }

        String facet = options.getString(QueryOptions.FACET);
        if (StringUtils.isEmpty(facet)) {
            throw new VariantQueryException("Empty facet query!");
        }
        try {
            return aggregation(query, options, facet);
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected abstract boolean canUseThisExecutor(Query query, QueryOptions options, String facet, List<String> reason) throws Exception;

    protected abstract VariantQueryResult<FacetField> aggregation(Query query, QueryOptions options, String facet) throws Exception;

}
