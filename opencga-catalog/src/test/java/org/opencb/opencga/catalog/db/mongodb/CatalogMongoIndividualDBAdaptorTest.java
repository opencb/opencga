package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogIndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Sample;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class CatalogMongoIndividualDBAdaptorTest extends CatalogMongoDBAdaptorTest {

    @Test
    public void testCreateIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(), null);
    }

    @Test
    public void testCreateIndividualStudyNotFound() throws Exception {
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.createIndividual(-10, new Individual(), null);
    }

    @Test
    public void testCreateIndividualFatherNotFound() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 10, -1, "", null, "", null, null,
                Collections.emptyList(), Collections.emptyMap()), null);
    }

    @Test
    public void testCreateIndividualAlreadyExists() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", -1, -1, "", null, "", null, null, Collections
                .emptyList(), null), null);
        thrown.expect(CatalogDBException.class); //Name already exists
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", -1, -1, "", null, "", null, null, Collections
                .emptyList(), null), null);
    }

    @Test
    public void testGetIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Gender.MALE, "", null, new Individual
                .Population(), Collections.emptyList(), null);
        individual = catalogIndividualDBAdaptor.createIndividual(studyId, individual, null).first();
        Individual individual2 = catalogIndividualDBAdaptor.getIndividual(individual.getId(), null).first();
        assertEquals(individual, individual2);
    }

    @Test
    public void testGetIndividualNoExists() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Gender.MALE, "", null, new Individual
                .Population(), Collections.emptyList(), null);
        catalogIndividualDBAdaptor.createIndividual(studyId, individual, null).first();
        catalogIndividualDBAdaptor.getIndividual(individual.getId(), null).first();
        thrown.expect(CatalogDBException.class); //Id not found
        catalogIndividualDBAdaptor.getIndividual(9999, null);
    }

    @Test
    public void testGetAllIndividuals() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_1", -1, -1, "Family1", Individual.Gender.MALE, "",
                null, new Individual.Population(), Collections.emptyList(), null), null);
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_2", -1, -1, "Family1", Individual.Gender.FEMALE, "",
                null, new Individual.Population(), Collections.emptyList(), null), null);
        long father = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_3", -1, -1, "Family2", Individual.Gender
                .MALE, "", null, new Individual.Population(), Collections.emptyList(), null), null).first().getId();
        long mother = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_4", -1, -1, "Family2", Individual.Gender
                .FEMALE, "", null, new Individual.Population(), Collections.emptyList(), null), null).first().getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_5", father, mother, "Family2", Individual.Gender
                .MALE, "", null, new Individual.Population(), Collections.emptyList(), null), null);
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "ind_6", -1, -1, "Family3", Individual.Gender.FEMALE, "",
                null, new Individual.Population(), Collections.emptyList(), null), null);

        QueryResult<Individual> result;
        result = catalogIndividualDBAdaptor.get(new Query(CatalogIndividualDBAdaptor.QueryParams.NAME.key(),
                "~ind_[1-3]").append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(CatalogIndividualDBAdaptor.QueryParams.GENDER.key(),
                Individual.Gender.FEMALE).append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(CatalogIndividualDBAdaptor.QueryParams.FAMILY.key(), "Family2")
                .append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(CatalogIndividualDBAdaptor.QueryParams.FATHER_ID.key(), ">0")
                .append(CatalogIndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testModifyIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        ObjectMap params = new ObjectMap("family", "new Family");
        params.append("gender", "MALE");
        catalogIndividualDBAdaptor.update(individualId, params);
        Individual individual = catalogIndividualDBAdaptor.getIndividual(individualId, null).first();
        assertEquals("new Family", individual.getFamily());
        assertEquals(Individual.Gender.MALE, individual.getGender());

    }

    @Test
    public void testModifyIndividualBadGender() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("gender", "bad gender"));
    }

    @Test
    public void testModifyIndividualBadFatherId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("fatherId", 4000));
    }

    @Test
    public void testModifyIndividualNegativeFatherId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        Individual individual = catalogIndividualDBAdaptor.update(individualId, new ObjectMap("fatherId", -1)).first();
        assertEquals(-1, individual.getFatherId());
    }

    @Test
    public void testModifyIndividualExistingName() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();
        catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in2", 0, 0, "", Individual.Gender.UNKNOWN, "", null,
                null, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("name", "in2"));
    }

    @Test
    public void testAnnotateIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        Set<Annotation> annotationSet = Arrays.asList(new Annotation("key", "value"), new Annotation("key2", "value2"), new Annotation
                ("key3", 3), new Annotation("key4", true)).stream().collect(Collectors.toSet());

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", Collections.emptyMap());
        catalogIndividualDBAdaptor.annotateIndividual(individualId, annot1, false);
        catalogIndividualDBAdaptor.annotateIndividual(individualId, annot2, false);

        Individual individual = catalogIndividualDBAdaptor.getIndividual(individualId, new QueryOptions()).first();
        Map<String, AnnotationSet> annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap
                (AnnotationSet::getName, Function.identity()));
        assertEquals(2, annotationSets.size());
        assertEquals(annot1, annotationSets.get(annot1.getName()));
        assertEquals(annot2, annotationSets.get(annot2.getName()));

        catalogIndividualDBAdaptor.deleteAnnotation(individualId, annot1.getName());

        individual = catalogIndividualDBAdaptor.getIndividual(individualId, new QueryOptions()).first();
        annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
        assertEquals(1, annotationSets.size());
        assertFalse(annotationSets.containsKey(annot1.getName()));
        assertEquals(annot2, annotationSets.get(annot2.getName()));

    }

    @Test
    public void testAnnotateIndividualExistingAnnotationId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        catalogIndividualDBAdaptor.annotateIndividual(individualId, new AnnotationSet("annot1", 3, Collections.<Annotation>emptySet(),
                "", Collections.emptyMap()), false);
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.annotateIndividual(individualId, new AnnotationSet("annot1", 3, Collections.<Annotation>emptySet(),
                "", Collections.emptyMap()), false);
    }

    @Test
    public void testAnnotateIndividualOverwriteExistingAnnotationId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v"), new Annotation("k2",
                "v2"))), "", Collections.emptyMap());
        QueryResult<AnnotationSet> queryResult = catalogIndividualDBAdaptor.annotateIndividual(individualId, annot1, false);
        assertEquals(annot1, queryResult.first());

        annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v2"), new Annotation("k3", "v3"))), "",
                Collections.emptyMap());
        queryResult = catalogIndividualDBAdaptor.annotateIndividual(individualId, annot1, true);
        assertEquals(annot1, queryResult.first());
    }

    @Test
    public void testAnnotateIndividualOverwriteNonExistingAnnotationId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v"), new Annotation("k2",
                "v2"))), "", Collections.emptyMap());
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.annotateIndividual(individualId, annot1, true);
    }


    @Test
    public void testDeleteIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();
        long individualId2 = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in2", 0, individualId, "", Individual
                .Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        catalogIndividualDBAdaptor.deleteIndividual(individualId2, null);
        catalogIndividualDBAdaptor.deleteIndividual(individualId, null);

    }

    @Test
    public void testDeleteIndividualInUse() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();
        long individualId2 = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in2", 0, individualId, "", Individual
                .Gender.UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.deleteIndividual(individualId, null);

    }

    @Test
    public void testDeleteIndividualInUseAsSample() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(0, "in1", 0, 0, "", Individual.Gender
                .UNKNOWN, "", null, null, Collections.emptyList(), null), null).first().getId();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().createSample(studyId, new Sample(0, "Sample", "", individualId, ""), null);

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.deleteIndividual(individualId, null);

    }

    @Test
    public void testGetStudyIdByIndividualId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.createIndividual(studyId, new Individual(), null).first().getId();
        long studyIdByIndividualId = catalogIndividualDBAdaptor.getStudyIdByIndividualId(individualId);
        assertEquals(studyId, studyIdByIndividualId);
    }

    @Test
    public void testNativeGet() throws Exception {
        long studyId = user4.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Gender.MALE, "", null, new Individual
                .Population(), Collections.emptyList(), null);
        individual = catalogIndividualDBAdaptor.createIndividual(studyId, individual, null).first();
        Individual individual2 = new Individual(0, "Another Individual", -1, -1, "Family2", Individual.Gender.FEMALE, "", null, new Individual
                .Population(), Collections.emptyList(), null);
        individual2 = catalogIndividualDBAdaptor.createIndividual(studyId, individual2, null).first();
        List<QueryResult> queryResults = catalogIndividualDBAdaptor.nativeGet(Arrays.asList(
                new Query(CatalogIndividualDBAdaptor.QueryParams.ID.key(), individual.getId()),
                new Query(CatalogIndividualDBAdaptor.QueryParams.ID.key(), individual2.getId())), new QueryOptions());

        assertEquals(2, queryResults.size());

        // Individual
        List<Document> results = queryResults.get(0).getResult();
        assertEquals(1, results.size());
        assertEquals("MALE", results.get(0).get("gender"));
        assertEquals("Family", results.get(0).get("family"));

        // Individual2
        results = queryResults.get(1).getResult();
        assertEquals(1, results.size());
        assertEquals("FEMALE", results.get(0).get("gender"));
        assertEquals("Family2", results.get(0).get("family"));
    }
}