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
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.SampleAcl;

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
    private CatalogDBAdaptorFactory dbAdaptorFactory;
    private CatalogSampleDBAdaptor catalogSampleDBAdaptor;
    private User user1;
    private User user2;
    private User user3;
    private User user4;
    private Sample s1;
    private Sample s2;
    private SampleAcl acl_s1_user1;
    private SampleAcl acl_s1_user2;
    private SampleAcl acl_s2_user1;
    private SampleAcl acl_s2_user2;

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

        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        acl_s1_user1 = new SampleAcl(Arrays.asList(user1.getId()), Arrays.asList());
        acl_s1_user2 = new SampleAcl(Arrays.asList(user2.getId()), Arrays.asList(
                SampleAcl.SamplePermissions.VIEW.name(),
                SampleAcl.SamplePermissions.VIEW_ANNOTATIONS.name(),
                SampleAcl.SamplePermissions.SHARE.name(),
                SampleAcl.SamplePermissions.UPDATE.name()
        ));
        s1 = catalogSampleDBAdaptor.createSample(studyId, new Sample(0, "s1", "", -1, "", Arrays.asList(acl_s1_user1, acl_s1_user2), null,
                null), null).first();
        acl_s2_user1 = new SampleAcl(Arrays.asList(user1.getId()), Arrays.asList());
        acl_s2_user2 = new SampleAcl(Arrays.asList(user2.getId()), Arrays.asList(
                SampleAcl.SamplePermissions.VIEW.name(),
                SampleAcl.SamplePermissions.VIEW_ANNOTATIONS.name(),
                SampleAcl.SamplePermissions.SHARE.name(),
                SampleAcl.SamplePermissions.UPDATE.name()
        ));
        s2 = catalogSampleDBAdaptor.createSample(studyId, new Sample(0, "s2", "", -1, "", Arrays.asList(acl_s2_user1, acl_s2_user2), null,
                null), null).first();

    }

    @Test
    public void testAnnotateSample() throws Exception {
        long sampleId = s1.getId();

        Set<Annotation> annotationSet = Arrays.asList(new Annotation("key", "value"), new Annotation("key2", "value2"), new Annotation
                ("key3", 3), new Annotation("key4", true)).stream().collect(Collectors.toSet());

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", Collections.emptyMap());
        catalogSampleDBAdaptor.annotateSample(sampleId, annot1, false);
        catalogSampleDBAdaptor.annotateSample(sampleId, annot2, false);

        Sample sample = catalogSampleDBAdaptor.getSample(sampleId, new QueryOptions()).first();
        Map<String, AnnotationSet> annotationSets = sample.getAnnotationSets().stream()
                .collect(Collectors.toMap(AnnotationSet::getId, Function.identity()));
        assertEquals(2, annotationSets.size());
        assertEquals(annot1, annotationSets.get(annot1.getId()));
        assertEquals(annot2, annotationSets.get(annot2.getId()));

        catalogSampleDBAdaptor.deleteAnnotation(sampleId, annot1.getId());

        sample = catalogSampleDBAdaptor.getSample(sampleId, new QueryOptions()).first();
        annotationSets = sample.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getId, Function.identity()));
        assertEquals(1, annotationSets.size());
        assertFalse(annotationSets.containsKey(annot1.getId()));
        assertEquals(annot2, annotationSets.get(annot2.getId()));

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
        catalogSampleDBAdaptor.annotateSample(sampleId, annot3, false);
        catalogSampleDBAdaptor.annotateSample(sampleId, annot1, false);
        catalogSampleDBAdaptor.annotateSample(sampleId, annot2, false);

        AnnotationSet annot4 = new AnnotationSet("annot4", 3, annotationSet, "", Collections.emptyMap());
        catalogSampleDBAdaptor.annotateSample(s2.getId(), annot4, false);
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

        catalogSampleDBAdaptor.annotateSample(sampleId, expectedAnnot, false);
        AnnotationSet annot = catalogSampleDBAdaptor.getSample(sampleId, null).first().getAnnotationSets().get(0);
        assertEquals(expectedAnnot, annot);

        annotationSet = Arrays.asList(
                new Annotation("key2", "value2"),
                new Annotation("key3", 8),
                new Annotation("key4", false),
                new Annotation("key5", 2.3))
                .stream().collect(Collectors.toSet());
        expectedAnnot = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        catalogSampleDBAdaptor.annotateSample(sampleId, expectedAnnot, true);
        annot = catalogSampleDBAdaptor.getSample(sampleId, null).first().getAnnotationSets().get(0);
        assertEquals(expectedAnnot, annot);

    }

    @Test
    public void getSampleAcl() throws Exception {
        QueryResult<SampleAcl> sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user1.getId());
        SampleAcl acl = sampleAcl.first();
        assertNotNull(acl);
        assertEquals(acl_s1_user1.getPermissions(), acl.getPermissions());

        acl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user2.getId()).first();
        assertNotNull(acl);
        assertEquals(acl_s1_user2.getPermissions(), acl.getPermissions());
    }

    @Test
    public void getSampleAclWrongUser() throws Exception {
        QueryResult<SampleAcl> wrongUser = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), "wrongUser");
        assertEquals(0, wrongUser.getNumResults());
    }

    @Test
    public void getSampleAclFromUserWithoutAcl() throws Exception {
        QueryResult<SampleAcl> sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user3.getId());
        assertTrue(sampleAcl.getResult().isEmpty());
    }

    @Test
    public void setSampleAclNew() throws Exception {
        SampleAcl acl_s1_user3 = new SampleAcl(Arrays.asList(user3.getId()), Collections.emptyList());

        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user3, true);
        QueryResult<SampleAcl> sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user3.getId());
        assertFalse(sampleAcl.getResult().isEmpty());
        assertEquals(acl_s1_user3.getPermissions(), sampleAcl.first().getPermissions());

        SampleAcl acl_s1_user4 = new SampleAcl(Arrays.asList(user4.getId()), Arrays.asList(SampleAcl.SamplePermissions.DELETE.name()));
        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user4, true);

        sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user4.getId());
        assertFalse(sampleAcl.getResult().isEmpty());
        assertEquals(acl_s1_user4.getPermissions(), sampleAcl.first().getPermissions());
    }

    // Remove all the permissions for the users
    @Test
    public void unsetSampleAcl() throws Exception {
        SampleAcl acl_s1_user3 = new SampleAcl(Arrays.asList(user3.getId()), Collections.emptyList());
        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user3, true);

        SampleAcl acl_s1_user4 = new SampleAcl(Arrays.asList(user4.getId()), Arrays.asList(SampleAcl.SamplePermissions.DELETE.name()));
        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user4, true);

        // Unset permissions
        catalogSampleDBAdaptor.unsetSampleAcl(s1.getId(), Arrays.asList(user3.getId(), user4.getId()), Collections.emptyList());
        QueryResult<SampleAcl> sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), Arrays.asList(user3.getId(), user4.getId()));
        assertEquals(0, sampleAcl.getNumResults());
    }

    // Remove some concrete permissions
    @Test
    public void unsetSampleAcl2() throws Exception {
        // Unset permissions
        QueryResult<SampleAcl> sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), Arrays.asList(user2.getId()));
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(4, sampleAcl.first().getPermissions().size());
        catalogSampleDBAdaptor.unsetSampleAcl(s1.getId(), Arrays.asList(user2.getId()),
                Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"));
        sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), Arrays.asList(user2.getId()));
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().getPermissions().size());
        assertTrue(sampleAcl.first().getPermissions().containsAll(Arrays.asList(SampleAcl.SamplePermissions.UPDATE,
                SampleAcl.SamplePermissions.SHARE)));

    }

    @Test
    public void setSampleAclOverride() throws Exception {
        assertEquals(acl_s1_user2.getPermissions(),
                catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user2.getId()).first().getPermissions());

        SampleAcl newAcl = new SampleAcl(Arrays.asList(user2.getId()), Arrays.asList(SampleAcl.SamplePermissions.DELETE.name()));
        assertTrue(!acl_s1_user2.getPermissions().equals(newAcl.getPermissions()));
        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), newAcl, true);

        assertEquals(newAcl.getPermissions(), catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user2.getId()).first().getPermissions());
    }

    @Test
    public void createSampleTest() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> result = dbAdaptorFactory.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);

        assertEquals(hg0097.getName(), result.first().getName());
        assertEquals(hg0097.getDescription(), result.first().getDescription());
        assertTrue(result.first().getId() > 0);
    }

    @Test
    public void deleteSampleTest() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);
        QueryResult<Sample> deleteResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId(),
                new QueryOptions());
        assertEquals(createResult.first().getId(), deleteResult.first().getId());
        assertEquals(1, deleteResult.getNumResults());
        assertEquals(Status.DELETED, deleteResult.first().getStatus().getStatus());
    }

    @Test
    public void deleteSampleFail1Test() throws Exception {
        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(55555555, new QueryOptions());
    }

    @Test
    public void deleteSampleFail2Test() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long fileId = dbAdaptorFactory.getCatalogFileDBAdaptor().getFileId(user3.getProjects().get(0).getStudies().get(0).getId(),
                "data/file.vcf");

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);
        dbAdaptorFactory.getCatalogFileDBAdaptor().update(fileId, new ObjectMap("sampleIds", createResult.first().getId()));

//        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId(), new QueryOptions());
        System.out.println("HOLA");
    }

    @Test
    public void deleteSampleFail3Test() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().createCohort(studyId, new Cohort("Cohort", Cohort.Type.COLLECTION, "", "",
                Collections.singletonList(createResult.first().getId()), null), null);

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
                        dbAdaptorFactory.getCatalogCohortDBAdaptor().createCohort(studyId, new Cohort(cohortName, Cohort.Type.COLLECTION,
                                "", "", Collections.emptyList(), null), null);
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
                new Query(CatalogCohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId), null).getResult();
        assertEquals(numCohorts, cohorts.size());
        Set<String> names = cohorts.stream().map(Cohort::getName).collect(Collectors.toSet());
        for (int c = 0; c < numCohorts; c++) {
            String cohortName = getCohortName.apply(c);
            names.contains(cohortName);
        }

    }

}
