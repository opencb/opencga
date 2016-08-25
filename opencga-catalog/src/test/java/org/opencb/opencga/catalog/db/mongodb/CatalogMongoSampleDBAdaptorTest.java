package org.opencb.opencga.catalog.db.mongodb;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created on 24/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogMongoSampleDBAdaptorTest {

    private CatalogDBAdaptorFactory dbAdaptorFactory;
    private CatalogSampleDBAdaptor catalogSampleDBAdaptor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private User user1;
    private User user2;
    private User user3;
    private Sample s1;
    private Sample s2;
    private AclEntry acl_s1_user1;
    private AclEntry acl_s1_user2;
    private AclEntry acl_s2_user1;
    private AclEntry acl_s2_user2;

    @Before
    public void before () throws IOException, CatalogDBException {
        CatalogMongoDBAdaptorTest dbAdaptorTest = new CatalogMongoDBAdaptorTest();
        dbAdaptorTest.before();

        user1 = CatalogMongoDBAdaptorTest.user1;
        user2 = CatalogMongoDBAdaptorTest.user2;
        user3 = CatalogMongoDBAdaptorTest.user3;
        dbAdaptorFactory = CatalogMongoDBAdaptorTest.catalogDBAdaptor;
        catalogSampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();

        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
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

    @AfterClass
    public static void afterClass() {
        CatalogMongoDBAdaptorTest.afterClass();
    }


    @Test
    public void testAnnotateSample() throws Exception {
        int sampleId = s1.getId();

        Set<Annotation> annotationSet = Arrays.asList(new Annotation("key", "value"), new Annotation("key2", "value2"), new Annotation("key3", 3), new Annotation("key4", true)).stream().collect(Collectors.toSet());

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", Collections.emptyMap());
        catalogSampleDBAdaptor.annotateSample(sampleId, annot1, false);
        catalogSampleDBAdaptor.annotateSample(sampleId, annot2, false);

        Sample sample = catalogSampleDBAdaptor.getSample(sampleId, new QueryOptions()).first();
        Map<String, AnnotationSet> annotationSets = sample.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getId, Function.identity()));
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
    public void testOverwriteAnnotateSample() throws Exception {
        int sampleId = s1.getId();

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

}
