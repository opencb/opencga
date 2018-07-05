package org.opencb.opencga.storage.core.manager;

import org.apache.solr.client.solrj.SolrServerException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.CohortMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.SampleMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogFamilyToSolrFamilyConverter;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogFileToSolrFileConverter;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogIndividualToSolrIndividualConverter;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;

import java.io.IOException;


/*
 * Created by wasim on 02/07/18.
*/

public class CatalogSolrManaerTest {


    @ClassRule
    public static CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static CatalogManager catalog;
    private static CatalogSolrManager catalogSolrManager;
    private static String sessionId;

    @BeforeClass
    public static void setUp() throws Exception {
        catalog = catalogManagerExternalResource.getCatalogManager();
        catalogSolrManager = new CatalogSolrManager(catalog);
    }


    @Test
    public void testIterator() throws CatalogException, SolrServerException, IOException, VariantSearchException {
        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalog.getConfiguration());
        CohortMongoDBAdaptor cohortMongoDBAdaptor = factory.getCatalogCohortDBAdaptor();
        DBIterator<Cohort> cohortIterator = cohortMongoDBAdaptor.iterator(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "samples"));

        while (cohortIterator.hasNext()){
            System.out.println(cohortIterator.next().getId());
        }


    }


    @Test
    public void getSamples() throws CatalogException, SolrServerException, IOException, VariantSearchException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "101");

        //  .append(Constants.ALL_VERSIONS, true);

        //  CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalog, null);
        // catalogSolrManager.insert(this.catalog.getSampleManager().get("phase1", new Query(), null, sessionId).getResult());


        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalog.getConfiguration());

      /*  SampleMongoDBAdaptor catalogSampleDBAdaptor = factory.getCatalogSampleDBAdaptor();
        CohortMongoDBAdaptor cohortMongoDBAdaptor = factory.getCatalogCohortDBAdaptor();
        FileMongoDBAdaptor fileMongoDBAdaptor = factory.getCatalogFileDBAdaptor();
        FamilyDBAdaptor familyDBAdaptor = factory.getCatalogFamilyDBAdaptor();*/
        IndividualDBAdaptor individualDBAdaptor = factory.getCatalogIndividualDBAdaptor();

        /*DBIterator<Sample> sampleIterator = catalogSampleDBAdaptor.iterator(new Query(), null);
        DBIterator<Cohort> cohortIterator = cohortMongoDBAdaptor.iterator(query, null);

        catalogSolrManager.insertCatalogCollection(sampleIterator, new CatalogSampleToSolrSampleConverter(), CatalogSolrManager.SAMPLES_SOLR_COLLECTION);
        catalogSolrManager.insertCatalogCollection(cohortIterator, new CatalogCohortToSolrCohortConverter(), CatalogSolrManager.COHORT_SOLR_COLLECTION);
*/
     //   DBIterator<File> fileIterator = fileMongoDBAdaptor.iterator(new Query(), null);
       // DBIterator<Family> familyIterator = familyDBAdaptor.iterator(new Query(), null);

       // catalogSolrManager.insertCatalogCollection(familyIterator, new CatalogFamilyToSolrFamilyConverter(), CatalogSolrManager.FAMILY_SOLR_COLLECTION);

        DBIterator<Individual> individualIterator = individualDBAdaptor.iterator(query, null);
       // System.out.println(individualDBAdaptor.count());
        catalogSolrManager.insertCatalogCollection(individualIterator, new CatalogIndividualToSolrIndividualConverter(), CatalogSolrManager.INDIVIDUAL_SOLR_COLLECTION);
        //catalogSolrManager.insertCatalogCollection(fileIterator, new CatalogFileToSolrFileConverter(), CatalogSolrManager.FILE_SOLR_COLLECTION);

        //  CohortMongoDBAdaptor cohortMongoDBAdaptor = factory.getCatalogCohortDBAdaptor();
        // QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE,"samples");

      /*  DBIterator<Cohort> iterator= cohortMongoDBAdaptor.iterator(query,null);
        List<Cohort> cohortList = new ArrayList<>();
        cohortList.add(iterator.next());
*/
        //cohortList.add(iterator.next());
        //   catalogSolrManager.insertCohorts(cohortMongoDBAdaptor.get(query,null).getResult());

    }
}

