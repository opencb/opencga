/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created on 24/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleMongoDBAdaptorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private DBAdaptorFactory dbAdaptorFactory;
    private SampleDBAdaptor catalogSampleDBAdaptor;
    private User user1;
    private User user2;
    private User user3;
    private User user4;
    private long studyId;
    private Sample s1;
    private Sample s2;

    @AfterClass
    public static void afterClass() {
        MongoDBAdaptorTest.afterClass();
    }

    @Before
    public void before() throws IOException, CatalogException {
        MongoDBAdaptorTest dbAdaptorTest = new MongoDBAdaptorTest();
        dbAdaptorTest.before();

        user1 = MongoDBAdaptorTest.user1;
        user2 = MongoDBAdaptorTest.user2;
        user3 = MongoDBAdaptorTest.user3;
        user4 = MongoDBAdaptorTest.user4;
        dbAdaptorFactory = MongoDBAdaptorTest.catalogDBAdaptor;
        catalogSampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();

        studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        s1 = catalogSampleDBAdaptor.insert(studyId, new Sample(0, "s1", "", new Individual(), "", "", false, 1, 1,
                Collections.emptyList(), new ArrayList<>(), Collections.emptyMap(), Collections.emptyMap()), null).first();
        s2 = catalogSampleDBAdaptor.insert(studyId, new Sample(0, "s2", "", new Individual(), "", "", false, 1, 1, Collections.emptyList(),
                new ArrayList<>(), Collections.emptyMap(), Collections.emptyMap()), null).first();
    }

//    @Test
//    public void testAnnotateSample() throws Exception {
//        long sampleId = s1.getId();
//
//        Map<String, Object> annotations = new HashMap<>();
//        annotations.put("key", "value");
//        annotations.put("key2", "value2");
//        annotations.put("key3", 3);
//        annotations.put("key4", true);
//
//        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotations, "", 1, Collections.emptyMap());
//        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotations, "", 1, Collections.emptyMap());
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot1);
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot2);
//
//        Sample sample = catalogSampleDBAdaptor.get(sampleId, new QueryOptions()).first();
//        Map<String, AnnotationSet> annotationSets = sample.getAnnotationSets().stream()
//                .collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
//        assertEquals(2, annotationSets.size());
//        assertEquals(annot1, annotationSets.get(annot1.getName()));
//        assertEquals(annot2, annotationSets.get(annot2.getName()));
//
//        catalogSampleDBAdaptor.deleteAnnotationSet(sampleId, annot1.getName());
//
//        sample = catalogSampleDBAdaptor.get(sampleId, new QueryOptions()).first();
//        annotationSets = sample.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
//        assertEquals(1, annotationSets.size());
//        assertFalse(annotationSets.containsKey(annot1.getName()));
//        assertEquals(annot2, annotationSets.get(annot2.getName()));
//
//    }

//    @Test
//    public void testAddVariableToAnnotations() throws Exception {
//        createAnnotationExample();
//        Variable variable = new Variable("ANOTHER_KEY", "", Variable.VariableType.BOOLEAN, false, true, false, null, -1, null, null, null,
//                null);
//        assertEquals(3, catalogSampleDBAdaptor.addVariableToAnnotations(3, variable).first().longValue());
//    }
//
//    @Test
//    public void testRenameFieldFromAnnotation() throws Exception {
//        createAnnotationExample();
//        // Rename
//        QueryResult<Long> longQueryResult = catalogSampleDBAdaptor.renameAnnotationField(3, "key4", "newKey4");
//        assertEquals(3, longQueryResult.first().longValue());
//
//        longQueryResult = catalogSampleDBAdaptor.renameAnnotationField(1, "key4", "newKey4");
//        assertEquals(0, longQueryResult.first().longValue());
//
//        longQueryResult = catalogSampleDBAdaptor.renameAnnotationField(3, "key4", "renewkey4");
//        assertEquals(0, longQueryResult.first().longValue());
//
//    }

//    private void createAnnotationExample() throws CatalogDBException {
//        long sampleId = s1.getId();
//
//        Map<String, Object> annotations = new HashMap<>();
//        annotations.put("key", "value");
//        annotations.put("key2", "value2");
//        annotations.put("key3", 3);
//        annotations.put("key4", true);
//
//        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotations, "", 1, Collections.emptyMap());
//        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotations, "", 1, Collections.emptyMap());
//        AnnotationSet annot3 = new AnnotationSet("annot3", 2, annotations, "", 1, Collections.emptyMap());
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot3);
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot1);
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot2);
//
//        AnnotationSet annot4 = new AnnotationSet("annot4", 3, annotations, "", 1, Collections.emptyMap());
//        catalogSampleDBAdaptor.createAnnotationSet(s2.getId(), , annot4);
//    }

//    @Test
//    public void testRemoveFieldFromAnnotations() throws Exception {
//        createAnnotationExample();
//        QueryResult<Long> longQueryResult = catalogSampleDBAdaptor.removeAnnotationField(3, "key4");
//        assertEquals(3, longQueryResult.first().longValue());
//
//        longQueryResult = catalogSampleDBAdaptor.removeAnnotationField(1, "key3");
//        assertEquals(0, longQueryResult.first().longValue());
//
//        longQueryResult = catalogSampleDBAdaptor.removeAnnotationField(3, "nonExistentKey");
//        assertEquals(0, longQueryResult.first().longValue());
//    }

//    @Test
//    public void testOverwriteAnnotateSample() throws Exception {
//        long sampleId = s1.getId();
//
//        Map<String, Object> annotations = new HashMap<>();
//        annotations.put("key", "value");
//        annotations.put("key2", "value2");
//        annotations.put("key3", 3);
//        annotations.put("key4", true);
//
//        AnnotationSet expectedAnnot = new AnnotationSet("annot1", 3, annotations, "", 1, Collections.emptyMap());
//
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , expectedAnnot);
//        AnnotationSet annot = catalogSampleDBAdaptor.get(sampleId, null).first().getAnnotationSets().get(0);
//        assertEquals(expectedAnnot, annot);
//
//        annotations = new HashMap<>();
//        annotations.put("key2", "value2");
//        annotations.put("key3", 8);
//        annotations.put("key4", false);
//        annotations.put("key5", 2.3);
//        expectedAnnot = new AnnotationSet("annot1", 3, annotations, "", 1, Collections.emptyMap());
//        catalogSampleDBAdaptor.updateAnnotationSet(sampleId, expectedAnnot);
//        annot = catalogSampleDBAdaptor.get(sampleId, null).first().getAnnotationSets().get(0);
//        assertEquals(expectedAnnot, annot);
//
//    }

    @Test
    public void searchByOntology() throws CatalogDBException {
        List<OntologyTerm> ontologyList = Arrays.asList(
                new OntologyTerm("hpo:123", "One hpo term", "hpo"),
                new OntologyTerm("hpo:456", "Another hpo term", "hpo"),
                new OntologyTerm("go:123", "My go term", "go")
        );
        Sample sample1 = new Sample().setName("sample1").setPhenotypes(ontologyList);

        ontologyList = Arrays.asList(
                new OntologyTerm("hpo:789", "One hpo term", "hpo"),
                new OntologyTerm("hpo:xxx", "Another hpo term", "hpo"),
                new OntologyTerm("hpo:456", "Another hpo term", "hpo"),
                new OntologyTerm("go:yyy", "My go term", "go")
        );
        Sample sample2 = new Sample().setName("sample2").setPhenotypes(ontologyList);

        catalogSampleDBAdaptor.insert(studyId, sample1, new QueryOptions()).first().getId();
        catalogSampleDBAdaptor.insert(studyId, sample2, new QueryOptions()).first().getId();

        // Start the search
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "=hpo:456,hpo:xxx")
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "=sample2");

        QueryResult<Sample> sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(sample2.getName(), sampleQueryResult.first().getName());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "hpo:456,hpo:xxx");
        sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleQueryResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "My go term");
        sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleQueryResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "go:123");
        sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(sample1.getName(), sampleQueryResult.first().getName());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "=hpo:456,My go term");
        sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleQueryResult.getNumResults());
    }

    @Test
    public void createSampleTest() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", new Individual(), "A description", 1);
        QueryResult<Sample> result = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, hg0097, null);

        assertEquals(hg0097.getName(), result.first().getName());
        assertEquals(hg0097.getDescription(), result.first().getDescription());
        assertTrue(result.first().getId() > 0);
    }

    @Test
    public void searchSampleByDateTest() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        // We create two samples "created" in different years
        Calendar calendar = Calendar.getInstance();
        calendar.set(2004, Calendar.MARCH, 24);
        Sample sample1 = new Sample(0, "sample1", "1000g", new Individual(), "A description", 1)
                .setCreationDate(TimeUtils.getTime(calendar.getTime()));

        calendar.set(2016, Calendar.DECEMBER, 17);
        Sample sample2 = new Sample(0, "sample2", "1000g", new Individual(), "A description", 1)
                .setCreationDate(TimeUtils.getTime(calendar.getTime()));

        dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, sample1, null);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, sample2, null);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.NAME.key());

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "~^sample");

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), ">2005");
        QueryResult<Sample> sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, options);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals("sample2", sampleQueryResult.first().getName());

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), ">200401");
        sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, options);
        assertEquals(2, sampleQueryResult.getNumResults());

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), "2003-2005");
        sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, options);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals("sample1", sampleQueryResult.first().getName());

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), "2003-2018");
        sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, options);
        assertEquals(2, sampleQueryResult.getNumResults());

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), "<201611");
        sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, options);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals("sample1", sampleQueryResult.first().getName());
    }


    // Test if we can search for samples of an individual
    @Test
    public void getSampleWithIndividual() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        QueryOptions queryOptions = new QueryOptions();

        // We create a new sample with the individual
        Sample sample = new Sample().setName("sample1");
        QueryResult<Sample> sampleQR = catalogSampleDBAdaptor.insert(studyId, sample, queryOptions);
        long sampleId = sampleQR.first().getId();

        // We create an individual
        String individualName = "individualName";
        String individualFamily = "Smith";
        Individual individual = new Individual()
                .setName(individualName)
                .setFamily(individualFamily)
                .setSamples(Arrays.asList(new Sample().setId(sampleId)));
        QueryResult<Individual> individualQueryResult = dbAdaptorFactory.getCatalogIndividualDBAdaptor().insert(studyId, individual, queryOptions);

        // Get the sample
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualQueryResult.first().getId());
        QueryResult<Sample> individualQuery = catalogSampleDBAdaptor.get(query, queryOptions);
        assertEquals("sample1", individualQuery.first().getName());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), -10);
        QueryResult<Sample> inexistentIndividualQuery = catalogSampleDBAdaptor.get(query, queryOptions);
        assertEquals(0, inexistentIndividualQuery.getNumResults());
    }


    @Test
    public void deleteSampleTest() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", new Individual(), "A description", 1);
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, hg0097, null);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId());

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("does not exist");
        catalogSampleDBAdaptor.get(createResult.first().getId(), QueryOptions.empty());
    }

    @Test
    public void deleteSampleFail1Test() throws Exception {
        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(55555555);
    }

    @Test
    @Ignore
    public void deleteSampleFail2Test() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long fileId = dbAdaptorFactory.getCatalogFileDBAdaptor().getId(user3.getProjects().get(0).getStudies().get(0).getId(),
                "data/file.vcf");

        Sample hg0097 = new Sample(0, "HG0097", "1000g", new Individual(), "A description", 1);
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, hg0097, null);
        dbAdaptorFactory.getCatalogFileDBAdaptor().update(fileId, new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(),
                createResult.first().getId()), QueryOptions.empty());

        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId());
    }

    @Test
    @Ignore
    // TODO: This should be tested in the sample manager, not here !!!
    public void deleteSampleFail3Test() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", new Individual(), "A description", 1);
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, hg0097, null);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().insert(studyId, new Cohort("Cohort", Study.Type.COLLECTION, "", "",
                Collections.singletonList(createResult.first()), 1, null), null);

        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId());
    }

    @Test
    public void createMultipleCohorts() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        AtomicInteger numFailures = new AtomicInteger();
        Function<Integer, String> getCohortName = c -> "Cohort_" + c;
        int numThreads = 10;
        int numCohorts = 10;
        for (int c = 0; c < numCohorts; c++) {
            List<Thread> threads = new LinkedList<>();
            String cohortName = getCohortName.apply(c);
            for (int i = 0; i < numThreads; i++) {
                threads.add(new Thread(() -> {
                    try {
                        dbAdaptorFactory.getCatalogCohortDBAdaptor().insert(studyId, new Cohort(cohortName, Study.Type.COLLECTION,
                                "", "", Collections.emptyList(), 1, null), null);
                    } catch (CatalogException ignore) {
                        numFailures.incrementAndGet();
                    }
                }));
            }
            threads.parallelStream().forEach(Thread::run);
            threads.parallelStream().forEach((thread) -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }


        assertEquals(numCohorts * numThreads - numCohorts, numFailures.intValue());
        List<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor().get(
                new Query(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId), null).getResult();
        assertEquals(numCohorts, cohorts.size());
        Set<String> names = cohorts.stream().map(Cohort::getName).collect(Collectors.toSet());
        for (int c = 0; c < numCohorts; c++) {
            String cohortName = getCohortName.apply(c);
            names.contains(cohortName);
        }

    }

}
