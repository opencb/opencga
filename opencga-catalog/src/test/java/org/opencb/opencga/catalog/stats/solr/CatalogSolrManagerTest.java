/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.stats.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.CohortMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.stats.solr.converters.*;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.study.StudyAclParams;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.user.Account;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.catalog.utils.ParamUtils.AclAction.ADD;

public class CatalogSolrManagerTest extends AbstractSolrManagerTest {

    private MongoDBAdaptorFactory factory;

    @Before
    public void before() throws CatalogDBException {
        factory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
    }

    @After
    public void after() {
        factory.close();
    }

    @Test
    public void testIterator() throws CatalogException, SolrServerException, IOException {
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

        QueryOptions queryOptions = new QueryOptions(ParamConstants.FLATTEN_ANNOTATIONS, "true");
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.STUDY_UID.key(),
                CohortDBAdaptor.QueryParams.TYPE.key(), CohortDBAdaptor.QueryParams.CREATION_DATE.key(), CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
                CohortDBAdaptor.QueryParams.RELEASE.key(), CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(), CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key()));

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
        SampleDBAdaptor sampleDBAdaptor = factory.getCatalogSampleDBAdaptor();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.add(QueryOptions.INCLUDE, Arrays.asList(SampleDBAdaptor.QueryParams.ID.key(),
                SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()));
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
    public void testIndividualIterator() throws CatalogException, SolrServerException, IOException {

        QueryOptions queryOptions = new QueryOptions(ParamConstants.FLATTEN_ANNOTATIONS, "true");
//        queryOptions.add("limit", 2);
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(IndividualDBAdaptor.QueryParams.UUID.key(),
                IndividualDBAdaptor.QueryParams.STUDY_UID.key(),
                IndividualDBAdaptor.QueryParams.SEX.key(), IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(),
                IndividualDBAdaptor.QueryParams.ETHNICITY.key(), IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                IndividualDBAdaptor.QueryParams.RELEASE.key(), IndividualDBAdaptor.QueryParams.CREATION_DATE.key(),
                IndividualDBAdaptor.QueryParams.INTERNAL_STATUS.key(), IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(),
                IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), IndividualDBAdaptor.QueryParams.PARENTAL_CONSANGUINITY.key(),
                IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key()));
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
    public void testCanViewIfAnonymousHavePermissions() throws CatalogException {
        // Grant permissions to anonymous user
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "*",
                new StudyAclParams(null, "view_only"), ADD, sessionIdOwner);

        study = catalogManager.getStudyManager().get(studyFqn, new QueryOptions(DBAdaptor.INCLUDE_ACLS, true), sessionIdOwner).first();

        Map<String, Set<String>> studyAcls =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
        // We replace the current studyAcls for the parsed one
        study.getAttributes().put("OPENCGA_ACL", studyAcls);

        QueryOptions queryOptions = new QueryOptions(ParamConstants.FLATTEN_ANNOTATIONS, true);
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(SampleDBAdaptor.QueryParams.UUID.key(),
                SampleDBAdaptor.QueryParams.STUDY_UID.key(),
                SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), SampleDBAdaptor.QueryParams.RELEASE.key(),
                SampleDBAdaptor.QueryParams.VERSION.key(), SampleDBAdaptor.QueryParams.CREATION_DATE.key(),
                SampleDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
                SampleDBAdaptor.QueryParams.SOMATIC.key(), SampleDBAdaptor.QueryParams.PHENOTYPES.key(),
                SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(),SampleDBAdaptor.QueryParams.UID.key(),
                SampleDBAdaptor.QueryParams.ATTRIBUTES.key()));
        queryOptions.append(DBAdaptor.INCLUDE_ACLS, true);

        SampleDBAdaptor sampleDBAdaptor = factory.getCatalogSampleDBAdaptor();
        DBIterator<Sample> sampleDBIterator = sampleDBAdaptor.iterator(
                new Query(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()), queryOptions);
        catalogSolrManager.insertCatalogCollection(sampleDBIterator, new CatalogSampleToSolrSampleConverter(study),
                CatalogSolrManager.SAMPLE_SOLR_COLLECTION);

        // We create a new user
        catalogManager.getUserManager().create("user4", "User4 Name", "user.2@e.mail", PASSWORD, "ACME", null, Account.AccountType.GUEST, null);

        // We query facets with the new user. That user should be able to see the information because anonymous can see it as well.
        DataResult<FacetField> facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, "creationYear>>creationMonth"), "user4");
        assertEquals(1, facet.getNumResults());
    }

    @Test
    public void testAnonymousDontView() throws CatalogException {
        study = catalogManager.getStudyManager().get(studyFqn, new QueryOptions(DBAdaptor.INCLUDE_ACLS, true), sessionIdOwner).first();

        Map<String, Set<String>> studyAcls =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
        // We replace the current studyAcls for the parsed one
        study.getAttributes().put("OPENCGA_ACL", studyAcls);

        QueryOptions queryOptions = new QueryOptions(ParamConstants.FLATTEN_ANNOTATIONS, true);
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(SampleDBAdaptor.QueryParams.UUID.key(),
                SampleDBAdaptor.QueryParams.STUDY_UID.key(),
                SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), SampleDBAdaptor.QueryParams.RELEASE.key(),
                SampleDBAdaptor.QueryParams.VERSION.key(), SampleDBAdaptor.QueryParams.CREATION_DATE.key(),
                SampleDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
                SampleDBAdaptor.QueryParams.SOMATIC.key(), SampleDBAdaptor.QueryParams.PHENOTYPES.key(),
                SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(),SampleDBAdaptor.QueryParams.UID.key(),
                SampleDBAdaptor.QueryParams.ATTRIBUTES.key()));
        queryOptions.append(DBAdaptor.INCLUDE_ACLS, true);

        SampleDBAdaptor sampleDBAdaptor = factory.getCatalogSampleDBAdaptor();
        DBIterator<Sample> sampleDBIterator = sampleDBAdaptor.iterator(
                new Query(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid()), queryOptions);
        catalogSolrManager.insertCatalogCollection(sampleDBIterator, new CatalogSampleToSolrSampleConverter(study),
                CatalogSolrManager.SAMPLE_SOLR_COLLECTION);

        // We query facets with the new user. That user should be able to see the information because anonymous can see it as well.
        DataResult<FacetField> facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, "creationYear>>creationMonth"), "*");
        assertEquals(0, facet.getNumResults());
    }

    @Test
    public void testInsertSamples() throws CatalogException, IOException {
        // Create annotationSet
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("a", "a", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                Collections.emptySet(), Collections.emptyMap()));
        variables.add(new Variable("a1", "a1", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                Collections.emptySet(), Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a2", "a2", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, false, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, true, null, null, 0, "", "",
                                                Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a3", "a3", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                                Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdOwner).first();

        InputStream inputStream = this.getClass().getClassLoader().getResource("annotation_sets/complete_annotation.json").openStream();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectMap annotations = objectMapper.readValue(inputStream, ObjectMap.class);

        catalogManager.getSampleManager().update(studyFqn, "sample1", new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdOwner);

        study = catalogManager.getStudyManager().get(studyFqn, new QueryOptions(DBAdaptor.INCLUDE_ACLS, true), sessionIdOwner).first();

        Map<String, Set<String>> studyAcls =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
        // We replace the current studyAcls for the parsed one
        study.getAttributes().put("OPENCGA_ACL", studyAcls);

        QueryOptions queryOptions = new QueryOptions(ParamConstants.FLATTEN_ANNOTATIONS, true);
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(SampleDBAdaptor.QueryParams.UUID.key(),
                SampleDBAdaptor.QueryParams.STUDY_UID.key(),
                SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), SampleDBAdaptor.QueryParams.RELEASE.key(),
                SampleDBAdaptor.QueryParams.VERSION.key(), SampleDBAdaptor.QueryParams.CREATION_DATE.key(),
                SampleDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
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
                new Query(), new QueryOptions(QueryOptions.FACET, "annotation.a.x"), "user1");
        assertEquals(1, facet.getResults().get(0).getBuckets().get(0).getCount());

        facet = catalogSolrManager.facetedQuery(study, CatalogSolrManager.SAMPLE_SOLR_COLLECTION,
                new Query(), new QueryOptions(QueryOptions.FACET, "annotation.a.x;id"), "user1");
        assertEquals(1, facet.getResults().get(0).getBuckets().get(0).getCount());
        assertEquals(2, facet.getResults().get(1).getBuckets().size());

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
        Map<String, Set<String>> studyAcls =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
        // We replace the current studyAcls for the parsed one
        study.getAttributes().put("OPENCGA_ACL", studyAcls);

        QueryOptions queryOptions = new QueryOptions(ParamConstants.FLATTEN_ANNOTATIONS, "true");
        //queryOptions.put("nativeQuery", true);
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.UUID.key(),
                FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.STUDY_UID.key(),
                FileDBAdaptor.QueryParams.TYPE.key(), FileDBAdaptor.QueryParams.FORMAT.key(),
                FileDBAdaptor.QueryParams.CREATION_DATE.key(), FileDBAdaptor.QueryParams.BIOFORMAT.key(),
                FileDBAdaptor.QueryParams.RELEASE.key(), FileDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
                FileDBAdaptor.QueryParams.EXTERNAL.key(), FileDBAdaptor.QueryParams.SIZE.key(),
                FileDBAdaptor.QueryParams.SOFTWARE.key(), FileDBAdaptor.QueryParams.EXPERIMENT.key(),
                FileDBAdaptor.QueryParams.RELATED_FILES.key(), FileDBAdaptor.QueryParams.SAMPLE_UIDS.key()));
        queryOptions.append(DBAdaptor.INCLUDE_ACLS, true);

        FileDBAdaptor fileDBAdaptor = factory.getCatalogFileDBAdaptor();
        DBIterator<File> fileDBIterator = fileDBAdaptor.iterator(new Query(), queryOptions);
        catalogSolrManager.insertCatalogCollection(fileDBIterator, new CatalogFileToSolrFileConverter(study),
                CatalogSolrManager.FILE_SOLR_COLLECTION);
    }

    @Test
    public void testInsertIndividuals() throws CatalogException, SolrServerException, IOException {
        Map<String, Set<String>> studyAcls =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
        // We replace the current studyAcls for the parsed one
        study.getAttributes().put("OPENCGA_ACL", studyAcls);

        QueryOptions queryOptions = new QueryOptions(ParamConstants.FLATTEN_ANNOTATIONS, "true");
        queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(IndividualDBAdaptor.QueryParams.UUID.key(),
                IndividualDBAdaptor.QueryParams.STUDY_UID.key(),
                IndividualDBAdaptor.QueryParams.SEX.key(), IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(),
                IndividualDBAdaptor.QueryParams.ETHNICITY.key(), IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                IndividualDBAdaptor.QueryParams.RELEASE.key(), IndividualDBAdaptor.QueryParams.CREATION_DATE.key(),
                IndividualDBAdaptor.QueryParams.INTERNAL_STATUS.key(), IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(),
                IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), IndividualDBAdaptor.QueryParams.PARENTAL_CONSANGUINITY.key(),
                IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key()));
        queryOptions.append(DBAdaptor.INCLUDE_ACLS, true);

        IndividualDBAdaptor individualDBAdaptor = factory.getCatalogIndividualDBAdaptor();
        DBIterator<Individual> individualDBIterator = individualDBAdaptor.iterator(new Query(), queryOptions);
        catalogSolrManager.insertCatalogCollection(individualDBIterator, new CatalogIndividualToSolrIndividualConverter(study),
                CatalogSolrManager.INDIVIDUAL_SOLR_COLLECTION);
    }

    @Test
    public void testInsertCohorts() throws CatalogException, SolrServerException, IOException {
        Map<String, Set<String>> studyAcls =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
        // We replace the current studyAcls for the parsed one
        study.getAttributes().put("OPENCGA_ACL", studyAcls);

        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(CohortDBAdaptor.QueryParams.UUID.key(),
                        CohortDBAdaptor.QueryParams.CREATION_DATE.key(), CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
                        CohortDBAdaptor.QueryParams.RELEASE.key(), CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key(),
                        CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), CohortDBAdaptor.QueryParams.TYPE.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(ParamConstants.FLATTEN_ANNOTATIONS, true);

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