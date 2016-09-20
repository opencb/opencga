package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
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
        catalogIndividualDBAdaptor.insert(new Individual(), studyId, null);
    }

    @Test
    public void testCreateIndividualStudyNotFound() throws Exception {
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.insert(new Individual(), -10, null);
    }

    @Test
    public void testCreateIndividualFatherNotFound() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 10, -1, "", null, "", null, null,
                Collections.emptyList(), Collections.emptyMap()), studyId, null);
    }

    @Test
    public void testCreateIndividualAlreadyExists() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.insert(new Individual(0, "in1", -1, -1, "", null, "", null, null, Collections
                .emptyList(), null), studyId, null);
        thrown.expect(CatalogDBException.class); //Name already exists
        catalogIndividualDBAdaptor.insert(new Individual(0, "in1", -1, -1, "", null, "", null, null, Collections
                .emptyList(), null), studyId, null);
    }

    @Test
    public void testGetIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Sex.MALE, "", null, new Individual
                .Population(), Collections.emptyList(), null);
        individual = catalogIndividualDBAdaptor.insert(individual, studyId, null).first();
        Individual individual2 = catalogIndividualDBAdaptor.get(individual.getId(), null).first();
        assertEquals(individual, individual2);
    }

    @Test
    public void testGetIndividualNoExists() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Sex.MALE, "", null, new Individual
                .Population(), Collections.emptyList(), null);
        catalogIndividualDBAdaptor.insert(individual, studyId, null).first();
        catalogIndividualDBAdaptor.get(individual.getId(), null).first();
        thrown.expect(CatalogDBException.class); //Id not found
        catalogIndividualDBAdaptor.get(9999, null);
    }

    @Test
    public void testGetAllIndividuals() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.insert(new Individual(0, "ind_1", -1, -1, "Family1", Individual.Sex.MALE, "",
                null, new Individual.Population(), Collections.emptyList(), null), studyId, null);
        catalogIndividualDBAdaptor.insert(new Individual(0, "ind_2", -1, -1, "Family1", Individual.Sex.FEMALE, "",
                null, new Individual.Population(), Collections.emptyList(), null), studyId, null);
        long father = catalogIndividualDBAdaptor.insert(new Individual(0, "ind_3", -1, -1, "Family2", Individual.Sex
                .MALE, "", null, new Individual.Population(), Collections.emptyList(), null), studyId, null).first().getId();
        long mother = catalogIndividualDBAdaptor.insert(new Individual(0, "ind_4", -1, -1, "Family2", Individual.Sex
                .FEMALE, "", null, new Individual.Population(), Collections.emptyList(), null), studyId, null).first().getId();
        catalogIndividualDBAdaptor.insert(new Individual(0, "ind_5", father, mother, "Family2", Individual.Sex
                .MALE, "", null, new Individual.Population(), Collections.emptyList(), null), studyId, null);
        catalogIndividualDBAdaptor.insert(new Individual(0, "ind_6", -1, -1, "Family3", Individual.Sex.FEMALE, "",
                null, new Individual.Population(), Collections.emptyList(), null), studyId, null);

        QueryResult<Individual> result;
        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.NAME.key(),
                "~ind_[1-3]").append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.SEX.key(),
                Individual.Sex.FEMALE).append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.FAMILY.key(), "Family2")
                .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), ">0")
                .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId), new QueryOptions());
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testModifyIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        ObjectMap params = new ObjectMap("family", "new Family");
        params.append("sex", "MALE");
        catalogIndividualDBAdaptor.update(individualId, params);
        Individual individual = catalogIndividualDBAdaptor.get(individualId, null).first();
        assertEquals("new Family", individual.getFamily());
        assertEquals(Individual.Sex.MALE, individual.getSex());

    }

    @Test
    public void testModifyIndividualBadGender() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("sex", "bad sex"));
    }

    @Test
    public void testModifyIndividualBadFatherId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("fatherId", 4000));
    }

    @Test
    public void testModifyIndividualNegativeFatherId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        Individual individual = catalogIndividualDBAdaptor.update(individualId, new ObjectMap("fatherId", -1)).first();
        assertEquals(-1, individual.getFatherId());
    }

    @Test
    public void testModifyIndividualExistingName() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();
        catalogIndividualDBAdaptor.insert(new Individual(0, "in2", 0, 0, "", Individual.Sex.UNKNOWN, "", null,
                null, Collections.emptyList(), null), studyId, null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("name", "in2"));
    }

    @Test
    public void testAnnotateIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        Set<Annotation> annotationSet = Arrays.asList(new Annotation("key", "value"), new Annotation("key2", "value2"), new Annotation
                ("key3", 3), new Annotation("key4", true)).stream().collect(Collectors.toSet());

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", Collections.emptyMap());
        catalogIndividualDBAdaptor.annotate(individualId, annot1, false);
        catalogIndividualDBAdaptor.annotate(individualId, annot2, false);

        Individual individual = catalogIndividualDBAdaptor.get(individualId, new QueryOptions()).first();
        Map<String, AnnotationSet> annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap
                (AnnotationSet::getName, Function.identity()));
        assertEquals(2, annotationSets.size());
        assertEquals(annot1, annotationSets.get(annot1.getName()));
        assertEquals(annot2, annotationSets.get(annot2.getName()));

        catalogIndividualDBAdaptor.deleteAnnotation(individualId, annot1.getName());

        individual = catalogIndividualDBAdaptor.get(individualId, new QueryOptions()).first();
        annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
        assertEquals(1, annotationSets.size());
        assertFalse(annotationSets.containsKey(annot1.getName()));
        assertEquals(annot2, annotationSets.get(annot2.getName()));

    }

    @Test
    public void testAnnotateIndividualExistingAnnotationId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        catalogIndividualDBAdaptor.annotate(individualId, new AnnotationSet("annot1", 3, Collections.<Annotation>emptySet(),
                "", Collections.emptyMap()), false);
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.annotate(individualId, new AnnotationSet("annot1", 3, Collections.<Annotation>emptySet(),
                "", Collections.emptyMap()), false);
    }

    @Test
    public void testAnnotateIndividualOverwriteExistingAnnotationId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v"), new Annotation("k2",
                "v2"))), "", Collections.emptyMap());
        QueryResult<AnnotationSet> queryResult = catalogIndividualDBAdaptor.annotate(individualId, annot1, false);
        assertEquals(annot1, queryResult.first());

        annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v2"), new Annotation("k3", "v3"))), "",
                Collections.emptyMap());
        queryResult = catalogIndividualDBAdaptor.annotate(individualId, annot1, true);
        assertEquals(annot1, queryResult.first());
    }

    @Test
    public void testAnnotateIndividualOverwriteNonExistingAnnotationId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, new HashSet<>(Arrays.asList(new Annotation("k", "v"), new Annotation("k2",
                "v2"))), "", Collections.emptyMap());
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.annotate(individualId, annot1, true);
    }


    @Test
    public void testDeleteIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();
        long individualId2 = catalogIndividualDBAdaptor.insert(new Individual(0, "in2", 0, individualId, "", Individual.Sex.UNKNOWN, "",
                null, null, Collections.emptyList(), null), studyId, null).first().getId();

        catalogIndividualDBAdaptor.delete(individualId2, new QueryOptions());
        catalogIndividualDBAdaptor.delete(individualId, new QueryOptions());
//        catalogIndividualDBAdaptor.deleteIndividual(individualId2, null);
//        catalogIndividualDBAdaptor.deleteIndividual(individualId, null);

    }

    @Test
    public void testDeleteIndividualInUse() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();
        catalogIndividualDBAdaptor.insert(new Individual(0, "in2", 0, individualId, "", Individual.Sex.UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.delete(individualId, new QueryOptions());

    }

    @Test
    public void testDeleteIndividualInUseAsSample() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, null, Collections.emptyList(), null), studyId, null).first().getId();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(new Sample(0, "Sample", "", individualId, ""), studyId, null);

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.delete(individualId, new QueryOptions());

    }

    @Test
    public void testGetStudyIdByIndividualId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(new Individual(), studyId, null).first().getId();
        long studyIdByIndividualId = catalogIndividualDBAdaptor.getStudyIdByIndividualId(individualId);
        assertEquals(studyId, studyIdByIndividualId);
    }

    @Test
    public void testNativeGet() throws Exception {
        long studyId = user4.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Sex.MALE, "", null, new Individual
                .Population(), Collections.emptyList(), null);
        individual = catalogIndividualDBAdaptor.insert(individual, studyId, null).first();
        Individual individual2 = new Individual(0, "Another Individual", -1, -1, "Family2", Individual.Sex.FEMALE, "", null, new Individual
                .Population(), Collections.emptyList(), null);
        individual2 = catalogIndividualDBAdaptor.insert(individual2, studyId, null).first();
        List<QueryResult> queryResults = catalogIndividualDBAdaptor.nativeGet(Arrays.asList(
                new Query(IndividualDBAdaptor.QueryParams.ID.key(), individual.getId()),
                new Query(IndividualDBAdaptor.QueryParams.ID.key(), individual2.getId())), new QueryOptions());

        assertEquals(2, queryResults.size());

        // Individual
        List<Document> results = queryResults.get(0).getResult();
        assertEquals(1, results.size());
        assertEquals("MALE", results.get(0).get("sex"));
        assertEquals("Family", results.get(0).get("family"));

        // Individual2
        results = queryResults.get(1).getResult();
        assertEquals(1, results.size());
        assertEquals("FEMALE", results.get(0).get("sex"));
        assertEquals("Family2", results.get(0).get("family"));
    }
}