package org.opencb.opencga.storage.core.variant.adaptors;

import com.google.common.base.Throwables;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.Arrays;
import org.opencb.opencga.storage.core.StorageEngineTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_PROTEIN_SUBSTITUTION_SCORES_COMPLETE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION;
import static org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager.SEARCH_ENGINE_ID;

/**
 * Created on 22/12/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@StorageEngineTest
public abstract class VariantQueryUsingSearchIndexTest extends VariantDBAdaptorTest {

    @ClassRule
    public static VariantSolrExternalResource solr = new VariantSolrExternalResource();

    @Override
    public void before() throws Exception {

        boolean preFileIndexed = VariantDBAdaptorTest.fileIndexed;
        super.before();

        solr.configure(variantStorageEngine);
        if (!preFileIndexed) {
            variantStorageEngine.secondaryIndex();
        }
    }


    @Override
    public VariantQueryResult<Variant> query(Query query, QueryOptions options) {
        try {
            if (options == null) {
                options = new QueryOptions();
            }
            options.put(VariantSearchManager.USE_SEARCH_INDEX, VariantStorageEngine.UseSearchIndex.YES);
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
            options.put(VariantSearchManager.USE_SEARCH_INDEX, VariantStorageEngine.UseSearchIndex.YES);
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
    public DataResult groupBy(Query query, String field, QueryOptions options) {
        try {
            return variantStorageEngine.groupBy(query, field, options);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public DataResult rank(int limit, Query query, String field, boolean asc) {
        try {
            return variantStorageEngine.rank(query, field, limit, asc);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    @Test
    public void testGetAlVariants_polyphenSiftDescription() {
        // Solr stores aggregated scores (max polyphen, min sift) and their descriptions.
        // Only the extreme-category descriptions are safe (no false negatives):
        //   polyphen: "probably damaging" (highest category — if any CT has it, max has it)
        //   sift: "deleterious" (lowest category — if any CT has it, min has it)
        // Skip this test entirely when per-CT scores become available.
        Assume.assumeFalse(SEARCH_PROTEIN_SUBSTITUTION_SCORES_COMPLETE.defaultValue());

        queryResult = query(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen=probably damaging"), null);
        assertThat(queryResult, everyResult(allVariantsSummary, hasAnnotation(hasAnyPolyphenDesc(equalTo("probably damaging")))));

        queryResult = query(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=deleterious"), null);
        assertThat(queryResult, everyResult(allVariantsSummary, hasAnnotation(hasAnySiftDesc(equalTo("deleterious")))));
    }

    @Test
    public void testQueryExecutor() {
        assertThat(variantStorageEngine.get(new VariantQuery(), new QueryOptions()
                        .append(QueryOptions.LIMIT, 10)
                        .append(QueryOptions.COUNT, false)
                        .append(QueryOptions.SKIP, 0)).getSource(),
                not(containsString(SEARCH_ENGINE_ID)));

        assertThat(variantStorageEngine.get(new VariantQuery(), new QueryOptions()
                        .append(QueryOptions.LIMIT, 10)
                        .append(QueryOptions.COUNT, true)
                        .append(QueryOptions.SKIP, 0)).getSource(),
                containsString(SEARCH_ENGINE_ID));

        assertThat(variantStorageEngine.get(new VariantQuery(), new QueryOptions()
                        .append(QueryOptions.LIMIT, 10)
                        .append(QueryOptions.COUNT, false)
                        .append(QueryOptions.SKIP, 100)).getSource(),
                not(containsString(SEARCH_ENGINE_ID)));

        assertThat(variantStorageEngine.get(new VariantQuery(), new QueryOptions()
                        .append(QueryOptions.LIMIT, 10)
                        .append(QueryOptions.COUNT, false)
                        .append(QueryOptions.SKIP, 1000)).getSource(),
                containsString(SEARCH_ENGINE_ID));

    }
}
