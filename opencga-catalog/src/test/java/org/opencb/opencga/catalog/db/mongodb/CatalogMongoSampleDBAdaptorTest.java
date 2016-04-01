package org.opencb.opencga.catalog.db.mongodb;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;

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
    private Sample s1;
    private Sample s2;
    private AclEntry acl_s1_user1;
    private AclEntry acl_s1_user2;
    private AclEntry acl_s2_user1;
    private AclEntry acl_s2_user2;

    @AfterClass
    public static void afterClass() {
        CatalogMongoDBAdaptorTest.afterClass();
    }

    @Before
    public void before() throws IOException, CatalogDBException {
        CatalogMongoDBAdaptorTest dbAdaptorTest = new CatalogMongoDBAdaptorTest();
        dbAdaptorTest.before();

        user1 = CatalogMongoDBAdaptorTest.user1;
        user2 = CatalogMongoDBAdaptorTest.user2;
        user3 = CatalogMongoDBAdaptorTest.user3;
        dbAdaptorFactory = CatalogMongoDBAdaptorTest.catalogDBAdaptor;
        catalogSampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();

        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        acl_s1_user1 = new AclEntry(user1.getId(), false, false, false, false);
        acl_s1_user2 = new AclEntry(user2.getId(), true, true, true, true);
        s1 = catalogSampleDBAdaptor.createSample(studyId, new Sample(0, "s1", "", -1, "", Arrays.asList(
                acl_s1_user1,
                acl_s1_user2
        ), null, null), null).first();
        acl_s2_user1 = new AclEntry(user1.getId(), false, false, false, false);
        acl_s2_user2 = new AclEntry(user2.getId(), true, true, true, true);
        s2 = catalogSampleDBAdaptor.createSample(studyId, new Sample(0, "s2", "", -1, "", Arrays.asList(
                acl_s2_user1,
                acl_s2_user2
        ), null, null), null).first();

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

        Variable variable = new Variable("ANOTHER_KEY", "", Variable.VariableType.BOOLEAN, false, true, false, null, -1, null, null, null,
                null);

        assertEquals(3, catalogSampleDBAdaptor.addVariableToAnnotations(3, variable).first().longValue());

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
        AclEntry acl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user1.getId()).first();
        assertNotNull(acl);
        assertEquals(acl_s1_user1, acl);

        acl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user2.getId()).first();
        assertNotNull(acl);
        assertEquals(acl_s1_user2, acl);
    }

    @Test
    public void getSampleAclWrongUser() throws Exception {
        thrown.expect(CatalogDBException.class);
        catalogSampleDBAdaptor.getSampleAcl(s1.getId(), "wrongUser");
    }

    @Test
    public void getSampleAclFromUserWithoutAcl() throws Exception {
        QueryResult<AclEntry> sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user3.getId());
        assertTrue(sampleAcl.getResult().isEmpty());
    }

    @Test
    public void setSampleAclNew() throws Exception {
        AclEntry acl_s1_user3 = new AclEntry(user3.getId(), false, false, false, false);

        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), acl_s1_user3);
        QueryResult<AclEntry> sampleAcl = catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user3.getId());
        assertFalse(sampleAcl.getResult().isEmpty());
        assertEquals(acl_s1_user3, sampleAcl.first());
    }

    @Test
    public void setSampleAclOverride() throws Exception {

        assertEquals(acl_s1_user2, catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user2.getId()).first());

        AclEntry newAcl = new AclEntry(user2.getId(), false, true, false, true);
        assertTrue(!acl_s1_user2.equals(newAcl));

        catalogSampleDBAdaptor.setSampleAcl(s1.getId(), newAcl);

        assertEquals(newAcl, catalogSampleDBAdaptor.getSampleAcl(s1.getId(), user2.getId()).first());

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
        QueryResult<Sample> deleteResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId(), false);
        assertEquals(createResult.first().getId(), deleteResult.first().getId());
        assertEquals(1, deleteResult.getNumResults());

        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().getSample(deleteResult.first().getId(), null);
    }

    @Test
    public void deleteSampleFail1Test() throws Exception {
        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(55555555, false);
    }

    @Test
    public void deleteSampleFail2Test() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long fileId = dbAdaptorFactory.getCatalogFileDBAdaptor().getFileId(user3.getProjects().get(0).getStudies().get(0).getId(),
                "data/file.vcf");

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);
        dbAdaptorFactory.getCatalogFileDBAdaptor().update(fileId, new ObjectMap("sampleIds", createResult.first().getId()));

        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId(), false);
    }

    @Test
    public void deleteSampleFail3Test() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> createResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);
        dbAdaptorFactory.getCatalogCohortDBAdaptor().createCohort(studyId, new Cohort("Cohort", Cohort.Type.COLLECTION, "", "",
                Collections.singletonList(createResult.first().getId()), null), null);

        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor().delete(createResult.first().getId(), false);
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
                    } catch (CatalogDBException ignore) {
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
        Study study = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudy(studyId, null).first();
        assertEquals(numCohorts, study.getCohorts().size());
        Set<String> names = study.getCohorts().stream().map(Cohort::getName).collect(Collectors.toSet());
        for (int c = 0; c < numCohorts; c++) {
            String cohortName = getCohortName.apply(c);
            names.contains(cohortName);
        }

    }

}
