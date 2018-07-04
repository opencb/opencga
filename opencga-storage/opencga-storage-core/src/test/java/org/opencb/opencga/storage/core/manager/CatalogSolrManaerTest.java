/*
package org.opencb.opencga.storage.core.manager;

import org.apache.solr.client.solrj.SolrServerException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.SampleMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;

import java.io.IOException;


*/
/**
 * Created by wasim on 02/07/18.
 *//*

public class CatalogSolrManaerTest {


    @ClassRule
    public static CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static CatalogManager catalog;
    private static StorageConfiguration storageConfiguration;
    private static CatalogSolrManager catalogSolrManager;
    private static String sessionId;

    @BeforeClass
    public static void setUp() throws Exception {
        catalog = catalogManagerExternalResource.getCatalogManager();
        storageConfiguration = opencga.getStorageConfiguration();
        catalogSolrManager = new CatalogSolrManager(catalog, storageConfiguration);

//        sessionId = catalog.getUserManager().login("user", "afds");
    }


    @Test
    public void indexSamplesTest() throws CatalogException, SolrServerException, IOException, VariantSearchException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.UID.key(), 4);

        catalogSolrManager.indexCatalogSamples(query);
    }


        @Test
    public void getSamples() throws CatalogException, SolrServerException, IOException, VariantSearchException {
    */
/*    Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "221001350_10002");*//*

        //  .append(Constants.ALL_VERSIONS, true);

      //  CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalog, null);
        // catalogSolrManager.insert(this.catalog.getSampleManager().get("phase1", new Query(), null, sessionId).getResult());*//*


        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalog.getConfiguration());

        SampleMongoDBAdaptor catalogSampleDBAdaptor = factory.getCatalogSampleDBAdaptor();
        catalogSolrManager.indexSamples(catalogSampleDBAdaptor.get(new Query(),null).getResult());



      //  CohortMongoDBAdaptor cohortMongoDBAdaptor = factory.getCatalogCohortDBAdaptor();
        // QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE,"samples");

       */
/* DBIterator<Cohort> iterator= cohortMongoDBAdaptor.iterator(query,null);
        List<Cohort> cohortList = new ArrayList<>();
        cohortList.add(iterator.next());*//*

        //cohortList.add(iterator.next());
        //   catalogSolrManager.insertCohorts(cohortMongoDBAdaptor.get(query,null).getResult());

    }
}

*/
