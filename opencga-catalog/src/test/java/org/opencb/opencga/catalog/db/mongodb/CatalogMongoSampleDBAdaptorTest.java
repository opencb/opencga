/*
 * Copyright 2015-2016 OpenCB
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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;

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
public class CatalogMongoSampleDBAdaptorTest {

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
    private SampleAclEntry acl_s1_user1;
    private SampleAclEntry acl_s1_user2;
    private SampleAclEntry acl_s2_user1;
    private SampleAclEntry acl_s2_user2;

    @AfterClass
    public static void afterClass() {
        CatalogMongoDBAdaptorTest.afterClass();
    }

    @Before
    public void before() throws IOException, CatalogException {
        CatalogMongoDBAdaptorTest dbAdaptorTest = new CatalogMongoDBAdaptorTest();
        dbAdaptorTest.before();

        user1 = CatalogMongoDBAdaptorTest.user1;
        user2 = CatalogMongoDBAdaptorTest.user2;
        user3 = CatalogMongoDBAdaptorTest.user3;
        user4 = CatalogMongoDBAdaptorTest.user4;
        dbAdaptorFactory = CatalogMongoDBAdaptorTest.catalogDBAdaptor;
        catalogSampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();

        studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        acl_s1_user1 = new SampleAclEntry(user1.getId(), Arrays.asList());
        acl_s1_user2 = new SampleAclEntry(user2.getId(), Arrays.asList(
                SampleAclEntry.SamplePermissions.VIEW.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.SHARE.name(),
                SampleAclEntry.SamplePermissions.UPDATE.name()
        ));
        s1 = catalogSampleDBAdaptor.insert(new Sample(0, "s1", "", new Individual(), "", Arrays.asList(acl_s1_user1, acl_s1_user2), null,
                null), studyId, null).first();
        acl_s2_user1 = new SampleAclEntry(user1.getId(), Arrays.asList());
        acl_s2_user2 = new SampleAclEntry(user2.getId(), Arrays.asList(
                SampleAclEntry.SamplePermissions.VIEW.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.SHARE.name(),
                SampleAclEntry.SamplePermissions.UPDATE.name()
        ));
        s2 = catalogSampleDBAdaptor.insert(new Sample(0, "s2", "", new Individual(), "", Arrays.asList(acl_s2_user1, acl_s2_user2), null,
                null), studyId, null).first();

    }

    @Test
    public void testAnnotateSample() throws Exception {
        long sampleId = s1.getId();

        Set<Annotation> annotationSet = Arrays.asList(new Annotation("key", "value"), new Annotation("key2", "value2"), new Annotation
                ("key3", 3), new Annotation("key4", true)).stream().collect(Collectors.toSet());

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", Collections.emptyMap());
        catalogSampleDBAdaptor.createAnnotationSet(sampleId, annot1);
        catalogSampleDBAdaptor.createAnnotationSet(sampleId, annot2);

        Sample sample = catalogSampleDBAdaptor.get(sampleId, new QueryOptions()).first();
        Map<String, AnnotationSet> annotationSets = sample.getAnnotationSets().stream()
                .collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
        assertEquals(2, annotationSets.size());
        assertEquals(annot1, annotationSets.get(annot1.getName()));
        assertEquals(annot2, annotationSets.get(annot2.getName()));

        catalogSampleDBAdaptor.deleteAnnotationSet(sampleId, annot1.getName());

        sample = catalogSampleDBAdaptor.get(sampleId, new QueryOptions()).first();
        annotationSets = sample.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
        assertEquals(1, annotationSets.size());
        assertFalse(annotationSets.containsKey(annot1.getName()));
        assertEquals(annot2, annotationSets.get(annot2.getName()));

    }

    @Test
    public void testAddVariableToAnnotations() throws Exception {
        createAnnotationExample();
        Variable variable = new Variable("ANOTHER_KEY", "", Variable.VariableType.BOOLEAN, false, true, false, null, -1, null, null, null,
                null);

        assertEquals(3, catalogSampleDBAdaptor.addVariableToAnnotations(3, variable).first().longValue());
    }

    @Test
    public void testRenameFieldFromAnnotation() throws Exception {
        createAnnotationExample();
        // Rename
        QueryResult<Long> longQueryResult = catalogSampleDBAdaptor.renameAnnotationField(3, "key4", "newKey4");
        assertEquals(3, longQueryResult.first().longValue());

        longQueryResult = catalogSampleDBAdaptor.renameAnnotationField(1, "key4", "newKey4");
        assertEquals(0, longQueryResult.first().longValue());

        longQueryResult = catalogSampleDBAdaptor.renameAnnotationField(3, "key4", "renewkey4");
        assertEquals(0, longQueryResult.first().longValue());

    }

    private void createAnnotationExample() throws CatalogDBException {
        long sampleId = s1.getId();

        Set<Annotation> annotationSet = Arrays.asList(new Annotation("key", "value"), new Annotation("key2", "value2"), new Annotation
                ("key3", 3), new Annotation("key4", true)).stream().collect(Collectors.toSet());

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", Collections.emptyMap());
        AnnotationSet annot3 = new AnnotationSet("annot3", 2, annotationSet, "", Collections.emptyMap());
        catalogSampleDBAdaptor.createAnnotationSet(sampleId, annot3);
        catalogSampleDBAdaptor.createAnnotationSet(sampleId, annot1);
        catalogSampleDBAdaptor.createAnnotationSet(sampleId, annot2);

        AnnotationSet annot4 = new AnnotationSet("annot4", 3, annotationSet, "", Collections.emptyMap());
        catalogSampleDBAdaptor.createAnnotationSet(s2.getId(), annot4);
    }

    @Test
    public void testRemoveFieldFromAnnotations() throws Exception {
        createAnnotationExample();
        QueryResult<Long> longQueryResult = catalogSampleDBAdaptor.removeAnnotationField(3, "key4");
        assertEquals(3, longQueryResult.first().longValue());

        longQueryResult = catalogSampleDBAdaptor.removeAnnotationField(1, "key3");
        assertEquals(0, longQueryResult.first().longValue());

        longQueryResult = catalogSampleDBAdaptor.removeAnnotationField(3, "nonExistentKey");
        assertEquals(0, longQueryResult.first().longValue());
    }

    @Test
    public void testOverwriteAnnotateSample() throws Exception {
        long sampleId = s1.getId();

        Set<Annotation> annotationSet = Arrays.asList(
                new Annotation("key", "value"),
                new Annotation("key2", "value2"),
                new Annotation("key3", 3),
                new Annotation("key4", true))
                .stream().collect(Collectors.toSet());
        AnnotationSet expectedAnnot = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());

        catalogSampleDBAdaptor.createAnnotationSet(sampleId, expectedAnnot);
        AnnotationSet annot = catalogSampleDBAdaptor.get(sampleId, null).first().getAnnotationSets().get(0);
        assertEquals(expectedAnnot, annot);

        annotationSet = Arrays.asList(
                new Annotation("key2", "value2"),
                new Annotation("key3", 8),
                new Annotation("key4", false),
                new Annotation("key5", 2.3))
                .stream().collect(Collectors.toSet());
        expectedAnnot = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        catalogSampleDBAdaptor.updateAnnotationSet(sampleId, expectedAnnot);
        annot = catalogSampleDBAdaptor.get(sampleId, null).first().getAnnotationSets().get(0);
        assertEquals(expectedAnnot, annot);

    }

    @Test
    public void searchByOntology() throws CatalogDBException {
        List<OntologyTerm> ontologyList = Arrays.asList(
                new OntologyTerm("hpo:123", "One hpo term", "hpo", "whatever", Collections.emptyMap()),
                new OntologyTerm("hpo:456", "Another hpo term", "hpo", "whatever", Collections.emptyMap()),
                new OntologyTerm("go:123", "My go term", "go", "whatever", Collections.emptyMap())
        );
        Sample sample1 = new Sample().setName("sample1").setOntologyTerms(ontologyList);

        ontologyList = Arrays.asList(
                new OntologyTerm("hpo:789", "One hpo term", "hpo", "whatever", Collections.emptyMap()),
                new OntologyTerm("hpo:xxx", "Another hpo term", "hpo", "whatever", Collections.emptyMap()),
                new OntologyTerm("hpo:456", "Another hpo term", "hpo", "whatever", Collections.emptyMap()),
                new OntologyTerm("go:yyy", "My go term", "go", "whatever", Collections.emptyMap())
        );
        Sample sample2 = new Sample().setName("sample2").setOntologyTerms(ontologyList);

        catalogSampleDBAdaptor.insert(sample1, studyId, new QueryOptions()).first().getId();
        catalogSampleDBAdaptor.insert(sample2, studyId, new QueryOptions()).first().getId();

        // Start the search
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ONTOLOGY_TERMS.key(), "=hpo:456,hpo:xxx")
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "=sample2");

        QueryResult<Sample> sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(sample2.getName(), sampleQueryResult.first().getName());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ONTOLOGY_TERMS.key(), "hpo:456,hpo:xxx");
        sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleQueryResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ONTOLOGY_TERMS.key(), "My go term");
        sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleQueryResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ONTOLOGIES.key(), "go:123");
        sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(sample1.getName(), sampleQueryResult.first().getName());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ONTOLOGIES.key(), "=hpo:456,My go term");
        sampleQueryResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleQueryResult.getNumResults());
    }

    @Test
    public void getSampleAcl() throws Exception {
        QueryResult<SampleAclEntry> sampleAcl = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user1.getId()));
        SampleAclEntry acl = sampleAcl.first();
        assertNotNull(acl);
        assertEquals(acl_s1_user1.getPermissions(), acl.getPermissions());

        acl = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user2.getId())).first();
        assertNotNull(acl);
        assertEquals(acl_s1_user2.getPermissions(), acl.getPermissions());
    }

    @Test
    public void getSampleAclWrongUser() throws Exception {
        QueryResult<SampleAclEntry> wrongUser = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList("wrongUser"));
        assertEquals(0, wrongUser.getNumResults());
    }

    @Test
    public void getSampleAclFromUserWithoutAcl() throws Exception {
        QueryResult<SampleAclEntry> sampleAcl = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user3.getId()));
        assertTrue(sampleAcl.getResult().isEmpty());
    }

    @Test
    public void setSampleAclNew() throws Exception {
        SampleAclEntry acl_s1_user3 = new SampleAclEntry(user3.getId(), Collections.emptyList());

        catalogSampleDBAdaptor.createAcl(s1.getId(), acl_s1_user3);
//        catalogSampleDBAdaptor.setAclsToMember(s1.getId(), user3.getId(), Collections.emptyList());
//        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user3, true);
        QueryResult<SampleAclEntry> sampleAcl = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user3.getId()));
        assertFalse(sampleAcl.getResult().isEmpty());
        assertEquals(acl_s1_user3.getPermissions(), sampleAcl.first().getPermissions());

        SampleAclEntry acl_s1_user4 = new SampleAclEntry(user4.getId(), Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()));
        catalogSampleDBAdaptor.createAcl(s1.getId(), acl_s1_user4);
//        catalogSampleDBAdaptor.setAclsToMember(s1.getId(), user4.getId(), Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()));
//        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user4, true);

        sampleAcl = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user4.getId()));
        assertFalse(sampleAcl.getResult().isEmpty());
        assertEquals(acl_s1_user4.getPermissions(), sampleAcl.first().getPermissions());
    }

    // Remove all the permissions for the users
    @Test
    public void unsetSampleAcl() throws Exception {
        SampleAclEntry acl_s1_user3 = new SampleAclEntry(user3.getId(), Collections.emptyList());

        catalogSampleDBAdaptor.createAcl(s1.getId(), acl_s1_user3);
//        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user3, true);

        SampleAclEntry acl_s1_user4 = new SampleAclEntry(user4.getId(), Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()));
//        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user4, true);
        catalogSampleDBAdaptor.createAcl(s1.getId(), acl_s1_user4);

        // Unset permissions
        catalogSampleDBAdaptor.removeAcl(s1.getId(), user3.getId());
        catalogSampleDBAdaptor.removeAcl(s1.getId(), user4.getId());
//        catalogSampleDBAdaptor.unsetSampleAcl(s1.getId(), Arrays.asList(user3.getId(), user4.getId()), Collections.emptyList());
        QueryResult<SampleAclEntry> sampleAcl = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user3.getId(), user4.getId()));
        assertEquals(0, sampleAcl.getNumResults());
    }

    // Remove some concrete permissions
    @Test
    public void unsetSampleAcl2() throws Exception {
        // Unset permissions
        QueryResult<SampleAclEntry> sampleAcl = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user2.getId()));
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(4, sampleAcl.first().getPermissions().size());
        catalogSampleDBAdaptor.removeAclsFromMember(s1.getId(), user2.getId(), Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"));
//        catalogSampleDBAdaptor.unsetSampleAcl(s1.getId(), Arrays.asList(user2.getId()),
//                Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"));
        sampleAcl = catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user2.getId()));
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().getPermissions().size());
        assertTrue(sampleAcl.first().getPermissions().containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.UPDATE,
                SampleAclEntry.SamplePermissions.SHARE)));

    }

    @Test
    public void setSampleAclOverride() throws Exception {
        assertEquals(acl_s1_user2.getPermissions(),
                catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user2.getId())).first().getPermissions());

        SampleAclEntry newAcl = new SampleAclEntry(user2.getId(), Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()));
        assertTrue(!acl_s1_user2.getPermissions().equals(newAcl.getPermissions()));
        catalogSampleDBAdaptor.setAclsToMember(s1.getId(), user2.getId(), Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()));
//        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), newAcl, true);

        assertEquals(newAcl.getPermissions(), catalogSampleDBAdaptor.getAcl(s1.getId(), Arrays.asList(user2.getId())).first().getPermissions());
    }

    @Test
    public void createSampleTest() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", new Individual(), "A description");
        QueryResult<Sample> result = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(hg0097, studyId, null);

        assertEquals(hg0097.getName(), result.first().getName());
        assertEquals(hg0097.getDescription(), result.first().getDescription());
        assertTrue(result.first().getId() > 0);
    }


    // Test if the lookup operation works fine
    @Test
    public void getSampleWithIndividual() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        QueryOptions queryOptions = new QueryOptions();

        // We create an individual
        String individualName = "individualName";
        String individualFamily = "Smith";
        Individual individual = new Individual().setName(individualName).setFamily(individualFamily);
        QueryResult<Individual> individualQR = dbAdaptorFactory.getCatalogIndividualDBAdaptor().insert(individual, studyId, queryOptions);

        // We create a new sample with the individual
        Sample sample = new Sample().setName("sample1").setIndividual(individualQR.first());
        QueryResult<Sample> sampleQR = catalogSampleDBAdaptor.insert(sample, studyId, queryOptions);

        // Get the sample
        QueryResult<Sample> noIndividualInfoQueryResult = catalogSampleDBAdaptor.get(sampleQR.first().getId(), queryOptions);
        assertNull(noIndividualInfoQueryResult.first().getIndividual().getName());
        assertNull(noIndividualInfoQueryResult.first().getIndividual().getFamily());

        queryOptions.put("lazy", false);
        QueryResult<Sample> individualInfoQueryResult = catalogSampleDBAdaptor.get(sampleQR.first().getId(), queryOptions);
        assertEquals(individualName, individualInfoQueryResult.first().getIndividual().getName());
        assertEquals(individualFamily, individualInfoQueryResult.first().getIndividual().getFamily());
    }

    // Test if the lookup operation still behaves well when no individual is associated to the sample
    @Test
    public void getSampleWithNoIndividual() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        QueryOptions queryOptions = new QueryOptions();

        Sample sample = new Sample().setName("sample1");
        QueryResult<Sample> sampleQR = catalogSampleDBAdaptor.insert(sample, studyId, queryOptions);

        // Get the sample
        QueryResult<Sample> noIndividualInfoQueryResult = catalogSampleDBAdaptor.get(sampleQR.first().getId(), queryOptions);
        assertNull(noIndividualInfoQueryResult.first().getIndividual().getName());
        assertNull(noIndividualInfoQueryResult.first().getIndividual().getFamily());

        queryOptions.put("lazy", false);
        QueryResult<Sample> individualInfoQueryResult = catalogSampleDBAdaptor.get(sampleQR.first().getId(), queryOptions);
        assertNull(individualInfoQueryResult.first().getIndividual().getName());
        assertNull(individualInfoQueryResult.first().getIndividual().getFamily());
    }


    @Test
    public void deleteSampleTest() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", new Individual(), "A description");
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(hg0097, studyId, null);
        QueryResult<Sample> deleteResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId(),
                new QueryOptions());
        assertEquals(createResult.first().getId(), deleteResult.first().getId());
        assertEquals(1, deleteResult.getNumResults());
        assertEquals(Status.DELETED, deleteResult.first().getStatus().getName());
    }

    @Test
    public void deleteSampleFail1Test() throws Exception {
        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(55555555, new QueryOptions());
    }

    @Test
    public void deleteSampleFail2Test() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long fileId = dbAdaptorFactory.getCatalogFileDBAdaptor().getId(user3.getProjects().get(0).getStudies().get(0).getId(),
                "data/file.vcf");

        Sample hg0097 = new Sample(0, "HG0097", "1000g", new Individual(), "A description");
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(hg0097, studyId, null);
        dbAdaptorFactory.getCatalogFileDBAdaptor().update(fileId, new ObjectMap("sampleIds", createResult.first().getId()));

//        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId(), new QueryOptions());
    }

    @Test
    public void deleteSampleFail3Test() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", new Individual(), "A description");
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(hg0097, studyId, null);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().insert(new Cohort("Cohort", Study.Type.COLLECTION, "", "",
                Collections.singletonList(createResult.first().getId()), null), studyId, null);

        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId(), new QueryOptions());
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
                        dbAdaptorFactory.getCatalogCohortDBAdaptor().insert(new Cohort(cohortName, Study.Type.COLLECTION,
                                "", "", Collections.emptyList(), null), studyId, null);
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
