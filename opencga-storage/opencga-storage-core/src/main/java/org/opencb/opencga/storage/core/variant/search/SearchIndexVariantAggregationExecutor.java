package org.opencb.opencga.storage.core.variant.search;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.query.executors.VariantAggregationExecutor;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SearchIndexVariantAggregationExecutor extends VariantAggregationExecutor {

    private final VariantSearchManager searchManager;
    private Logger logger = LoggerFactory.getLogger(SearchIndexVariantAggregationExecutor.class);


    public SearchIndexVariantAggregationExecutor(VariantSearchManager searchManager) {
        this.searchManager = searchManager;
    }

    @Override
    protected boolean canUseThisExecutor(Query query, QueryOptions options, String facet, List<String> reason) throws Exception {
        return VariantSearchUtils.isQueryCovered(query);
    }

    @Override
    protected VariantQueryResult<FacetField> aggregation(Query query, QueryOptions options, String facet) throws Exception {
        SearchIndexMetadata indexMetadata = searchManager.getSearchIndexMetadata();
        DataResult<FacetField> r = searchManager.facetedQuery(indexMetadata, query, options);
        return new VariantQueryResult<>(r)
                .setSource(VariantSearchManager.SEARCH_ENGINE_ID);
    }
}
