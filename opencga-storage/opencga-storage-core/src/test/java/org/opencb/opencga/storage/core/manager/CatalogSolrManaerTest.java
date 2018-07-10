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
import org.opencb.opencga.catalog.stats.solr.converters.*;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;

import java.io.IOException;

import static org.opencb.opencga.catalog.utils.Constants.FLATTENED_ANNOTATIONS;


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
        FileMongoDBAdaptor fileMongoDBAdaptor = factory.getCatalogFileDBAdaptor();

        QueryOptions queryOptions = new QueryOptions(QueryOptions.LIMIT, "10000");
        queryOptions.add(QueryOptions.EXCLUDE, "samples");
        queryOptions.add(QueryOptions.EXCLUDE, "attributes");
        queryOptions.add("allowDiskUse", "true");


        DBIterator<File> fileDBIterator = fileMongoDBAdaptor.iterator(new Query(), queryOptions);

        while (fileDBIterator.hasNext()) {
            System.out.println(fileDBIterator.next().getId());

        }


    }


    @Test
    public void testFacet() throws CatalogException, SolrServerException, IOException, VariantSearchException {

        catalogSolrManager.facetedQuery(catalog.getConfiguration().getDatabasePrefix() +"_"+ CatalogSolrManager.SAMPLES_SOLR_COLLECTION, new Query(), new QueryOptions(QueryOptions.FACET, "annotations__s__GermlineSample__GermlineSample__source"));

    }

    @Test
    public void getSamples() throws CatalogException, SolrServerException, IOException, VariantSearchException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "101");

        QueryOptions queryOptions = new QueryOptions(FLATTENED_ANNOTATIONS, "true");
        queryOptions.add(QueryOptions.EXCLUDE,"samples");
       // queryOptions.add(QueryOptions.LIMIT,1000);
        //  .append(Constants.ALL_VERSIONS, true);

        //  CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalog, null);
        // catalogSolrManager.insert(this.catalog.getSampleManager().get("phase1", new Query(), null, sessionId).getResult());


        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalog.getConfiguration());


        SampleMongoDBAdaptor catalogSampleDBAdaptor = factory.getCatalogSampleDBAdaptor();
        CohortMongoDBAdaptor cohortMongoDBAdaptor = factory.getCatalogCohortDBAdaptor();
        FileMongoDBAdaptor fileMongoDBAdaptor = factory.getCatalogFileDBAdaptor();
        FamilyDBAdaptor familyDBAdaptor = factory.getCatalogFamilyDBAdaptor();
         IndividualDBAdaptor individualDBAdaptor = factory.getCatalogIndividualDBAdaptor();
//        DBIterator<File> fileIterator = fileMongoDBAdaptor.iterator(new Query(), new QueryOptions(QueryOptions.LIMIT, "10"));
  //      catalogSolrManager.insertCatalogCollection(fileIterator, new CatalogFileToSolrFileConverter(), CatalogSolrManager.FILE_SOLR_COLLECTION);

        DBIterator<Sample> sampleIterator = catalogSampleDBAdaptor.iterator(new Query(), queryOptions);
        //DBIterator<Cohort> cohortIterator = cohortMongoDBAdaptor.iterator(new Query(), queryOptions);
       // DBIterator<Cohort> cohortIterator = cohortMongoDBAdaptor.iterator(new Query(), new QueryOptions(QueryOptions.EXCLUDE, "samples"));
        catalogSolrManager.insertCatalogCollection(sampleIterator, new CatalogSampleToSolrSampleConverter(), CatalogSolrManager.SAMPLES_SOLR_COLLECTION);
       // catalogSolrManager.insertCatalogCollection(cohortIterator, new CatalogCohortToSolrCohortConverter(), CatalogSolrManager.COHORT_SOLR_COLLECTION);

     //   DBIterator<File> fileIterator = fileMongoDBAdaptor.iterator(new Query(), queryOptions);
        //DBIterator<Family> familyIterator = familyDBAdaptor.iterator(new Query(), null);

        // catalogSolrManager.insertCatalogCollection(familyIterator, new CatalogFamilyToSolrFamilyConverter(), CatalogSolrManager.FAMILY_SOLR_COLLECTION);

          //DBIterator<Individual> individualIterator = individualDBAdaptor.iterator(new Query(), queryOptions);
        // System.out.println(individualDBAdaptor.count());
        //catalogSolrManager.insertCatalogCollection(individualIterator, new CatalogIndividualToSolrIndividualConverter(), CatalogSolrManager.INDIVIDUAL_SOLR_COLLECTION);
     //   catalogSolrManager.insertCatalogCollection(fileIterator, new CatalogFileToSolrFileConverter(), CatalogSolrManager.FILE_SOLR_COLLECTION);

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

