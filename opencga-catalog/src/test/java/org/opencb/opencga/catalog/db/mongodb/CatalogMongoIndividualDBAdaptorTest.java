package org.opencb.opencga.catalog.db.mongodb;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class CatalogMongoIndividualDBAdaptorTest {

    private CatalogDBAdaptorFactory dbAdaptorFactory;
    private CatalogIndividualDBAdaptor catalogIndividualDBAdaptor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private User user1;
    private User user2;
    private User user3;

    @Before
    public void before () throws IOException, CatalogDBException {
        CatalogMongoDBAdaptorTest dbAdaptorTest = new CatalogMongoDBAdaptorTest();
        dbAdaptorTest.before();

        user1 = CatalogMongoDBAdaptorTest.user1;
        user2 = CatalogMongoDBAdaptorTest.user2;
        user3 = CatalogMongoDBAdaptorTest.user3;
        dbAdaptorFactory = CatalogMongoDBAdaptorTest.catalogDBAdaptor;
        catalogIndividualDBAdaptor = dbAdaptorFactory.getCatalogIndividualDBAdaptor();

    }

    @AfterClass
    public static void afterClass() {
        CatalogMongoDBAdaptorTest.afterClass();
    }

    @Test
    public void testCreateIndividual() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(), null);
    }

    @Test
    public void testCreateIndividualStudyNotFound() throws Exception {
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.createIndividual(-10, new Individual(), null);
    }

    @Test
    public void testCreateIndividualFatherNotFound() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 10, -1, "", null, "", null, null, Collections.emptyList(), null), null);
    }
    @Test
    public void testCreateIndividualAlreadyExists() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", -1, -1, "", null, "", null, null, Collections.emptyList(), null), null);
        thrown.expect(CatalogDBException.class); //Name already exists
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", -1, -1, "", null, "", null, null, Collections.emptyList(), null), null);
    }

    @Test
    public void testGetIndividual() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Gender.MALE, "", null, new Individual.Population(), Collections.emptyList(), null);
        individual = catalogIndividualDBAdaptor.createIndividual(studyId, individual, null).first();
        Individual individual2 = catalogIndividualDBAdaptor.getIndividual(individual.getId(), null).first();
        assertEquals(individual, individual2);
    }

    @Test
    public void testGetIndividualNoExists() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Gender.MALE, "", null, new Individual.Population(), Collections.emptyList(), null);
        catalogIndividualDBAdaptor.createIndividual(studyId, individual, null).first();
        catalogIndividualDBAdaptor.getIndividual(individual.getId(), null).first();
        thrown.expect(CatalogDBException.class); //Id not found
        catalogIndividualDBAdaptor.getIndividual(9999, null);
    }

    @Test
    public void testGetAllIndividuals() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_1", -1, -1, "Family1", Individual.Gender.MALE, "", null, new Individual.Population(), Collections.emptyList(), null), null);
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_2", -1, -1, "Family1", Individual.Gender.FEMALE, "", null, new Individual.Population(), Collections.emptyList(), null), null);
        int father = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_3", -1, -1, "Family2", Individual.Gender.MALE, "", null, new Individual.Population(), Collections.emptyList(), null), null).first().getId();
        int mother = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_4", -1, -1, "Family2", Individual.Gender.FEMALE, "", null, new Individual.Population(), Collections.emptyList(), null), null).first().getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_5", father, mother, "Family2", Individual.Gender.MALE, "", null, new Individual.Population(), Collections.emptyList(), null), null);
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_6", -1, -1, "Family3", Individual.Gender.FEMALE, "", null, new Individual.Population(), Collections.emptyList(), null), null);

        QueryResult<Individual> result;
        result = catalogIndividualDBAdaptor.getAllIndividuals(new QueryOptions(CatalogIndividualDBAdaptor.IndividualFilterOption.name.toString(), "~ind_[1-3]").append(CatalogIndividualDBAdaptor.IndividualFilterOption.studyId.toString(), studyId));
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.getAllIndividuals(new QueryOptions(CatalogIndividualDBAdaptor.IndividualFilterOption.gender.toString(), Individual.Gender.FEMALE).append(CatalogIndividualDBAdaptor.IndividualFilterOption.studyId.toString(), studyId));
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.getAllIndividuals(new QueryOptions(CatalogIndividualDBAdaptor.IndividualFilterOption.family.toString(), "Family2").append(CatalogIndividualDBAdaptor.IndividualFilterOption.studyId.toString(), studyId));
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.getAllIndividuals(new QueryOptions(CatalogIndividualDBAdaptor.IndividualFilterOption.fatherId.toString(), ">0").append(CatalogIndividualDBAdaptor.IndividualFilterOption.studyId.toString(), studyId));
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testModifyIndividual() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        QueryOptions params = new QueryOptions("family", "new Family");
        params.add("gender", "MALE");
        catalogIndividualDBAdaptor.modifyIndividual(individualId, params);
        Individual individual = catalogIndividualDBAdaptor.getIndividual(individualId, null).first();
        assertEquals("new Family", individual.getFamily());
        assertEquals(Individual.Gender.MALE, individual.getGender());

    }

    @Test
    public void testModifyIndividualBadGender() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.modifyIndividual(individualId, new QueryOptions("gender", "bad gender"));
    }

    @Test
    public void testModifyIndividualBadFatherId() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.modifyIndividual(individualId, new QueryOptions("fatherId", 4000));
    }

    @Test
    public void testModifyIndividualNegativeFatherId() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        Individual individual = catalogIndividualDBAdaptor.modifyIndividual(individualId, new QueryOptions("fatherId", -1)).first();
        assertEquals(-1, individual.getFatherId());
    }

    @Test
    public void testModifyIndividualExistingName() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in2", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.modifyIndividual(individualId, new QueryOptions("name", "in2"));
    }

    @Test
    public void testAnnotateIndividual() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        Set<Annotation> annotationSet = Arrays.asList(new Annotation("key", "value"), new Annotation("key2", "value2"), new Annotation("key3", 3), new Annotation("key4", true)).stream().collect(Collectors.toSet());

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", Collections.emptyMap());
        catalogIndividualDBAdaptor.annotateIndividual(individualId, annot1, false);
        catalogIndividualDBAdaptor.annotateIndividual(individualId, annot2, false);

        Individual individual = catalogIndividualDBAdaptor.getIndividual(individualId, new QueryOptions()).first();
        Map<String, AnnotationSet> annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getId, Function.identity()));
        assertEquals(2, annotationSets.size());
        assertEquals(annot1, annotationSets.get(annot1.getId()));
        assertEquals(annot2, annotationSets.get(annot2.getId()));

        catalogIndividualDBAdaptor.deleteAnnotation(individualId, annot1.getId());

        individual = catalogIndividualDBAdaptor.getIndividual(individualId, new QueryOptions()).first();
        annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getId, Function.identity()));
        assertEquals(1, annotationSets.size());
        assertFalse(annotationSets.containsKey(annot1.getId()));
        assertEquals(annot2, annotationSets.get(annot2.getId()));

    }

    @Test
    public void testAnnotateIndividualExistingAnnotationId() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        catalogIndividualDBAdaptor.annotateIndividual(individualId, new AnnotationSet("annot1", 3, Collections.<Annotation>emptySet(), "", Collections.emptyMap()), false);
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.annotateIndividual(individualId, new AnnotationSet("annot1", 3, Collections.<Annotation>emptySet(), "", Collections.emptyMap()), false);
    }

    @Test
    public void testAnnotateIndividualOverwriteExistingAnnotationId() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v"), new Annotation("k2", "v2"))), "", Collections.emptyMap());
        QueryResult<AnnotationSet> queryResult = catalogIndividualDBAdaptor.annotateIndividual(individualId, annot1, false);
        assertEquals(annot1, queryResult.first());

        annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v2"), new Annotation("k3", "v3"))), "", Collections.emptyMap());
        queryResult = catalogIndividualDBAdaptor.annotateIndividual(individualId, annot1, true);
        assertEquals(annot1, queryResult.first());
    }

    @Test
    public void testAnnotateIndividualOverwriteNonExistingAnnotationId() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v"), new Annotation("k2", "v2"))), "", Collections.emptyMap());
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.annotateIndividual(individualId, annot1, true);
    }


    @Test
    public void testDeleteIndividual() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();
        int individualId2 = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in2", 0, individualId, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        catalogIndividualDBAdaptor.deleteIndividual(individualId2, null);
        catalogIndividualDBAdaptor.deleteIndividual(individualId, null);

    }

    @Test
    public void testDeleteIndividualInUse() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();
        int individualId2 = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in2", 0, individualId, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.deleteIndividual(individualId, null);

    }

    @Test
    public void testDeleteIndividualInUseAsSample() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();
        dbAdaptorFactory.getCatalogSampleDBAdaptor().createSample(studyId, new Sample(0, "Sample", "", individualId, ""), null);

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.deleteIndividual(individualId, null);

    }

    @Test
    public void testGetStudyIdByIndividualId() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(), null).first().getId();
        int studyIdByIndividualId = catalogIndividualDBAdaptor.getStudyIdByIndividualId(individualId);
        assertEquals(studyId, studyIdByIndividualId);
    }

}