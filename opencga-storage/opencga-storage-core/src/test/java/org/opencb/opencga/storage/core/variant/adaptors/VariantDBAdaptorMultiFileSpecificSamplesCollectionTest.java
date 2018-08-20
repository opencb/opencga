package org.opencb.opencga.storage.core.variant.adaptors;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUtils;
import org.opencb.opencga.storage.core.variant.solr.SolrExternalResource;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created on 23/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantDBAdaptorMultiFileSpecificSamplesCollectionTest extends VariantDBAdaptorMultiFileTest {

    @ClassRule
    public static SolrExternalResource solr = new SolrExternalResource();

    @Before
    public void before() throws Exception {
        solr.configure(variantStorageEngine);
        super.before();
    }

    @Override
    protected void load() throws Exception {
        super.load();

        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
        for (String studyName : scm.getStudyNames(null)) {
            StudyConfiguration sc = scm.getStudyConfiguration(studyName, null).first();
            ArrayList<String> samples = new ArrayList<>(sc.getSampleIds().keySet());
            samples.sort(String::compareTo);
            variantStorageEngine.searchIndexSamples(sc.getStudyName(), samples.subList(0, samples.size() / 2));
            variantStorageEngine.searchIndexSamples(sc.getStudyName(), samples.subList(samples.size() / 2, samples.size()));
        }
    }


    protected VariantQueryResult<Variant> query(Query query, QueryOptions options) {
        try {
            StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
            String collection = VariantSearchUtils.inferSpecificSearchIndexSamplesCollection(query, options, scm, DB_NAME);

            // Do not execute this test if the query is not covered by the specific search index collection
            Assume.assumeThat(query.toJson(), collection, CoreMatchers.notNullValue());

            if (options.getInt(QueryOptions.LIMIT, 0) <= 0) {
                options = new QueryOptions(options);
                options.put(QueryOptions.LIMIT, 100000);
            }

            return variantStorageEngine.getVariantSearchManager().query(collection, query, options);
        } catch (StorageEngineException | VariantSearchException | IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return null;
    }

}

