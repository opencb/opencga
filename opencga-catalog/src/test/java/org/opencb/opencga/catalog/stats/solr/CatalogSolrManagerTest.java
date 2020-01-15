package org.opencb.opencga.catalog.stats.solr;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Test;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.CohortMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogCohortToSolrCohortConverter;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogSampleToSolrSampleConverter;
import org.opencb.opencga.catalog.stats.solr.converters.SolrConverterUtil;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.catalog.utils.Constants.FLATTENED_ANNOTATIONS;

public class CatalogSolrManagerTest extends AbstractSolrManagerTest {

    @Test
    public void testIterator() throws CatalogException, SolrServerException, IOException {
        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
        FileMongoDBAdaptor fileMongoDBAdaptor = factory.getCatalogFileDBAdaptor();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.add(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.SAMPLE_UIDS.key()));
        //queryOptions.add("nativeQuery", true);

        DBIterator<File> fileDBIterator = fileMongoDBAdaptor.iterator(new Query("uid", 1000000154L), queryOptions);
        boolean found = false;
        while (fileDBIterator.hasNext()) {
            if (fileDBIterator.next().getSamples().size() > 0) {
                System.out.println("found");
                found = true;
            }
        }
        if (!found) {
            System.out.println("nothing");
        }

    }

    @Test
    public void testCohortIterator() throws CatalogException, SolrServerException, IOException {

        QueryOptions queryOptions = new QueryOptions(FLATTENED_ANNOTATIONS, "true");
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.NAME.key(),
                CohortDBAdaptor.QueryParams.STUDY_UID.key(),
                CohortDBAdaptor.QueryParams.TYPE.key(), CohortDBAdaptor.QueryParams.CREATION_DATE.key(), CohortDBAdaptor.QueryParams.STATUS.key(),
                CohortDBAdaptor.QueryParams.RELEASE.key(), CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(), CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key()));

        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
        CohortMongoDBAdaptor cohortMongoDBAdaptor = factory.getCatalogCohortDBAdaptor();
        DBIterator<Cohort> cohortIterator = cohortMongoDBAdaptor.iterator(new Query(), queryOptions);
        int i = 0;
        StopWatch stopWatch = StopWatch.createStarted();
        while (cohortIterator.hasNext()) {
            cohortIterator.next();
            i++;
            if (i % 10000 == 0) {
                System.out.println("i: " + i + "; time: " + stopWatch.getTime(TimeUnit.MILLISECONDS));
                stopWatch.reset();
                stopWatch.start();
                return;
            }
        }
    }

    @Test
    public void testSampleIterator() throws CatalogException, SolrServerException, IOException {
        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
        SampleDBAdaptor sampleDBAdaptor = factory.getCatalogSampleDBAdaptor();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.add(QueryOptions.INCLUDE, Arrays.asList(SampleDBAdaptor.QueryParams.ID.key(),
                SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.INDIVIDUAL.key() + ".id"));
        //queryOptions.add("nativeQuery", true);
        queryOptions.add("lazy", false);

        DBIterator<Sample> sampleDBIterator = sampleDBAdaptor.iterator(new Query(), queryOptions);
        int i = 0;
        StopWatch stopWatch = StopWatch.createStarted();
        while (sampleDBIterator.hasNext()) {
            Sample sample = sampleDBIterator.next();
            i++;
            if (i % 10000 == 0) {
                System.out.println("i: " + i + "; time: " + stopWatch.getTime(TimeUnit.MILLISECONDS));
                stopWatch.reset();
                stopWatch.start();
            }
//            System.out.println(sample);
//            if (sample.getAttributes() != null && sample.getAttributes().containsKey("individual")) {
//                System.out.println(sample.getAttributes().get("individual"));
//            }
        }
    }

    @Test
    public void testIndividusalIterator() throws CatalogException, SolrServerException, IOException {

        QueryOptions queryOptions = new QueryOptions(FLATTENED_ANNOTATIONS, "true");
//        queryOptions.add("limit", 2);
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(IndividualDBAdaptor.QueryParams.ID.key(),
                IndividualDBAdaptor.QueryParams.STUDY_UID.key(), IndividualDBAdaptor.QueryParams.MULTIPLES.key(),
                IndividualDBAdaptor.QueryParams.SEX.key(), IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(),
                IndividualDBAdaptor.QueryParams.ETHNICITY.key(), IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                IndividualDBAdaptor.QueryParams.RELEASE.key(), IndividualDBAdaptor.QueryParams.CREATION_DATE.key(),
                IndividualDBAdaptor.QueryParams.STATUS.key(), IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(),
                IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(), IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), IndividualDBAdaptor.QueryParams.PARENTAL_CONSANGUINITY.key(),
                IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key()));
        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
        IndividualDBAdaptor individualDBAdaptor = factory.getCatalogIndividualDBAdaptor();
        DBIterator<Individual> individualDBIterator = individualDBAdaptor.iterator(new Query(), queryOptions);
        int i = 0;
        StopWatch stopWatch = StopWatch.createStarted();
        while (individualDBIterator.hasNext()) {
//            System.out.println(individualDBIterator.next().getId() + " : " + i++);
            individualDBIterator.next();
            i++;
            if (i % 10000 == 0) {
                System.out.println("i: " + i + "; time: " + stopWatch.getTime(TimeUnit.MILLISECONDS));
                stopWatch.reset();
                stopWatch.start();
            }
        }
    }

    @Test
    public void testInsertSamples() throws CatalogException, IOException {
        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());

        Map<String, Set<String>> studyAcls =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
        // We replace the current studyAcls for the parsed one
        study.getAttributes().put("OPENCGA_ACL", studyAcls);

        QueryOptions queryOptions = new QueryOptions(FLATTENED_ANNOTATIONS, true);
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(SampleDBAdaptor.QueryParams.ID.key(),

                SampleDBAdaptor.QueryParams.STUDY_UID.key(), SampleDBAdaptor.QueryParams.SOURCE.key(),
                SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), SampleDBAdaptor.QueryParams.RELEASE.key(),
                SampleDBAdaptor.QueryParams.VERSION.key(), SampleDBAdaptor.QueryParams.CREATION_DATE.key(),
                SampleDBAdaptor.QueryParams.STATUS.key(), SampleDBAdaptor.QueryParams.TYPE.key(),
                SampleDBAdaptor.QueryParams.SOMATIC.key(), SampleDBAdaptor.QueryParams.PHENOTYPES.key(),
                SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(),SampleDBAdaptor.QueryParams.UID.key(),
                SampleDBAdaptor.QueryParams.ATTRIBUTES.key()));
        queryOptions.append(DBAdaptor.INCLUDE_ACLS, true);

        SampleDBAdaptor sampleDBAdaptor = factory.getCatalogSampleDBAdaptor();
        DBIterator<Sample> sampleDBIterator = sampleDBAdaptor.iterator(
                new Query(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()), queryOptions);
        catalogSolrManager.insertCatalogCollection(sampleDBIterator, new CatalogSampleToSolrSampleConverter(study),
                CatalogSolrManager.SAMPLE_SOLR_COLLECTION);
        DataResult<FacetField> facet;
//        facet = catalogSolrManager.facetedQuery(CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
//                new Query(), new QueryOptions(QueryOptions.FACET, SampleDBAdaptor.QueryParams.RELEASE.key()));
//        assertEquals(3, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, SampleDBAdaptor.QueryParams.RELEASE.key()), "user1");
        assertEquals(2, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, SampleDBAdaptor.QueryParams.RELEASE.key()), "user2");
        assertEquals(1, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, SampleDBAdaptor.QueryParams.RELEASE.key()), "user3");
        assertEquals(1, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, SampleDBAdaptor.QueryParams.RELEASE.key()), "admin1");
        assertEquals(3, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, SampleDBAdaptor.QueryParams.RELEASE.key()), "owner");
        assertEquals(3, facet.getResults().get(0).getBuckets().get(0).getCount());
    }

    @Test
    public void testInsertFiles() throws CatalogException, SolrServerException, IOException {

        QueryOptions queryOptions = new QueryOptions(FLATTENED_ANNOTATIONS, "true");
        //queryOptions.put("nativeQuery", true);
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.STUDY_UID.key(),
                FileDBAdaptor.QueryParams.TYPE.key(), FileDBAdaptor.QueryParams.FORMAT.key(),
                FileDBAdaptor.QueryParams.CREATION_DATE.key(), FileDBAdaptor.QueryParams.BIOFORMAT.key(),
                FileDBAdaptor.QueryParams.RELEASE.key(), FileDBAdaptor.QueryParams.STATUS.key(),
                FileDBAdaptor.QueryParams.EXTERNAL.key(), FileDBAdaptor.QueryParams.SIZE.key(),
                FileDBAdaptor.QueryParams.SOFTWARE.key(), FileDBAdaptor.QueryParams.EXPERIMENT_UID.key(),
                FileDBAdaptor.QueryParams.RELATED_FILES.key(), FileDBAdaptor.QueryParams.SAMPLE_UIDS.key()));

        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
        FileDBAdaptor fileDBAdaptor = factory.getCatalogFileDBAdaptor();
        DBIterator<File> fileDBIterator = fileDBAdaptor.iterator(new Query(), queryOptions);
//        catalogSolrManager.insertCatalogCollection(fileDBIterator, new CatalogFileToSolrFileConverter(), CatalogSolrManager.FILE_SOLR_COLLECTION);
    }

    @Test
    public void testInsertIndividuals() throws CatalogException, SolrServerException, IOException {

        QueryOptions queryOptions = new QueryOptions(FLATTENED_ANNOTATIONS, "true");
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(IndividualDBAdaptor.QueryParams.ID.key(),
                IndividualDBAdaptor.QueryParams.STUDY_UID.key(), IndividualDBAdaptor.QueryParams.MULTIPLES.key(),
                IndividualDBAdaptor.QueryParams.SEX.key(), IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(),
                IndividualDBAdaptor.QueryParams.ETHNICITY.key(), IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                IndividualDBAdaptor.QueryParams.RELEASE.key(), IndividualDBAdaptor.QueryParams.CREATION_DATE.key(),
                IndividualDBAdaptor.QueryParams.STATUS.key(), IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(),
                IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(), IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), IndividualDBAdaptor.QueryParams.PARENTAL_CONSANGUINITY.key(),
                IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key()));
        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
        IndividualDBAdaptor individualDBAdaptor = factory.getCatalogIndividualDBAdaptor();
        DBIterator<Individual> individualDBIterator = individualDBAdaptor.iterator(new Query(), queryOptions);
//        catalogSolrManager.insertCatalogCollection(individualDBIterator, new CatalogIndividualToSolrIndividualConverter(),
//                CatalogSolrManager.INDIVIDUAL_SOLR_COLLECTION);
    }

    @Test
    public void testInsertCohorts() throws CatalogException, SolrServerException, IOException {
        MongoDBAdaptorFactory factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());

        Map<String, Set<String>> studyAcls =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
        // We replace the current studyAcls for the parsed one
        study.getAttributes().put("OPENCGA_ACL", studyAcls);

        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.NAME.key(),
                        CohortDBAdaptor.QueryParams.CREATION_DATE.key(), CohortDBAdaptor.QueryParams.STATUS.key(),
                        CohortDBAdaptor.QueryParams.RELEASE.key(), CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(),
                        CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), CohortDBAdaptor.QueryParams.TYPE.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(Constants.FLATTENED_ANNOTATIONS, true);

        CohortDBAdaptor cohortDBAdaptor = factory.getCatalogCohortDBAdaptor();
        DBIterator<Cohort> cohortDBIterator = cohortDBAdaptor.iterator(
                new Query(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()), queryOptions);
        catalogSolrManager.insertCatalogCollection(cohortDBIterator, new CatalogCohortToSolrCohortConverter(study),
                CatalogSolrManager.COHORT_SOLR_COLLECTION);
        DataResult<FacetField> facet = catalogSolrManager.facetedQuery(CatalogSolrManager.COHORT_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, CohortDBAdaptor.QueryParams.RELEASE.key()));
        assertEquals(3, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.COHORT_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, CohortDBAdaptor.QueryParams.RELEASE.key()), "user1");
        assertEquals(2, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.COHORT_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, CohortDBAdaptor.QueryParams.RELEASE.key()), "user2");
        assertEquals(1, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.COHORT_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, CohortDBAdaptor.QueryParams.RELEASE.key()), "user3");
        assertEquals(1, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.COHORT_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, CohortDBAdaptor.QueryParams.RELEASE.key()), "admin1");
        assertEquals(3, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.COHORT_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, CohortDBAdaptor.QueryParams.RELEASE.key()), "owner");
        assertEquals(3, facet.getResults().get(0).getBuckets().get(0).getCount());
    }


}