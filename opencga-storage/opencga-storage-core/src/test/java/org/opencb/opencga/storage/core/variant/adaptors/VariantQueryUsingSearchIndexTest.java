package org.opencb.opencga.storage.core.variant.adaptors;

import com.google.common.base.Throwables;
import org.junit.ClassRule;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.solr.SolrExternalResource;

/**
 * Created on 22/12/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantQueryUsingSearchIndexTest extends VariantDBAdaptorTest {

    @ClassRule
    public static SolrExternalResource solr = new SolrExternalResource();

    @Override
    public void before() throws Exception {

        boolean preFileIndexed = VariantDBAdaptorTest.fileIndexed;
        super.before();

        solr.configure(variantStorageEngine);
        if (!preFileIndexed) {
            variantStorageEngine.searchIndex();
        }
    }


    @Override
    public VariantQueryResult<Variant> query(Query query, QueryOptions options) {
        try {
            if (options == null) {
                options = new QueryOptions();
            }
            options.put(VariantSearchManager.USE_SEARCH_INDEX, VariantSearchManager.UseSearchIndex.YES);
            return variantStorageEngine.get(query, options);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        try {
            if (options == null) {
                options = new QueryOptions();
            }
            options.put(VariantSearchManager.USE_SEARCH_INDEX, VariantSearchManager.UseSearchIndex.YES);
            return variantStorageEngine.iterator(query, options);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Long count(Query query) {
        try {
            return variantStorageEngine.count(query).first();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        try {
            return variantStorageEngine.groupBy(query, field, options);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public QueryResult rank(int limit, Query query, String field, boolean asc) {
        try {
            return variantStorageEngine.rank(query, field, limit, asc);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
