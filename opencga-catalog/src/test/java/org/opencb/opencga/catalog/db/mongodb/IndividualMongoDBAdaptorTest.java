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

import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualMongoDBAdaptorTest extends MongoDBAdaptorTest {

    @Test
    public void testCreateIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.insert(studyId, new Individual(), null);
    }

    @Test
    public void testCreateIndividualStudyNotFound() throws Exception {
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.insert(-10, new Individual(), null);
    }

    @Test
    public void testCreateIndividualFatherNotFound() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 10, -1, "", null, "", null, 1, Collections.emptyList(), Collections.emptyMap()), null);
    }

    @Test
    public void testCreateIndividualAlreadyExists() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", -1, -1, "", null, "", null, 1, Collections
                .emptyList(), null), null);
        thrown.expect(CatalogDBException.class); //Name already exists
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", -1, -1, "", null, "", null, 1, Collections
                .emptyList(), null), null);
    }

    @Test
    public void testGetIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Sex.MALE, "",
                new Individual.Population(), 1, Collections.emptyList(), null);
        individual = catalogIndividualDBAdaptor.insert(studyId, individual, null).first();
        Individual individual2 = catalogIndividualDBAdaptor.get(individual.getId(), null).first();
        assertEquals(individual.toString(), individual2.toString());
    }

    @Test
    public void testGetIndividualNoExists() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Sex.MALE, "", new Individual
                .Population(), 1, Collections.emptyList(), null);
        catalogIndividualDBAdaptor.insert(studyId, individual, null).first();
        catalogIndividualDBAdaptor.get(individual.getId(), null).first();
        thrown.expect(CatalogDBException.class); //Id not found
        catalogIndividualDBAdaptor.get(9999, null);
    }

    @Test
    public void testGetAllIndividuals() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "ind_1", -1, -1, "Family1", Individual.Sex.MALE, "", new Individual.Population(), 1, Collections.emptyList(), null), null);
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "ind_2", -1, -1, "Family1", Individual.Sex.FEMALE, "", new Individual.Population(), 1, Collections.emptyList(), null), null);

        Individual father = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "ind_3", -1, -1, "Family2", Individual.Sex
                .MALE, "", new Individual.Population(), 1, Collections.emptyList(), null), null).first();
        Individual mother = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "ind_4", -1, -1, "Family2", Individual.Sex
                .FEMALE, "", new Individual.Population(), 1, Collections.emptyList(), null), null).first();
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "ind_5", father, mother, null, Individual.Sex.MALE, Individual.KaryotypicSex.XY,
                "", new Individual.Population(), null, null, null, null, true, 1, Collections.emptyList(), null).setFamily("Family2"), null);
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "ind_6", -1, -1, "Family3", Individual.Sex.FEMALE, "",
                new Individual.Population(), 1, Collections.emptyList(), null), null);

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
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        ObjectMap params = new ObjectMap("family", "new Family");
        params.append("sex", "MALE");
        catalogIndividualDBAdaptor.update(individualId, params, QueryOptions.empty());
        Individual individual = catalogIndividualDBAdaptor.get(individualId, null).first();
        assertEquals("new Family", individual.getFamily());
        assertEquals(Individual.Sex.MALE, individual.getSex());

    }

    @Test
    public void testModifyIndividualBadGender() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("sex", "bad sex"), QueryOptions.empty());
    }

    @Test
    public void testModifyIndividualBadFatherId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("fatherId", 4000), QueryOptions.empty());
    }

    @Test
    public void testModifyIndividualNegativeFatherId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        Individual individual = catalogIndividualDBAdaptor.update(individualId,
                new ObjectMap(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), -1), QueryOptions.empty()).first();
        assertEquals(-1, individual.getFather().getId());
    }

    // FIXME: This should be tested in the managers
    @Ignore
    @Test
    public void testModifyIndividualExistingName() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in2", 0, 0, "", Individual.Sex.UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualId, new ObjectMap("name", "in2"), QueryOptions.empty());
    }

    @Test
    public void testAvoidDuplicatedSamples() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual()
                .setName("in2")
                .setSamples(Arrays.asList(new Sample().setId(5), new Sample().setId(5), new Sample().setId(7), new Sample().setId(-1)));
        Individual individualStored = catalogIndividualDBAdaptor.insert(studyId, individual, null).first();
        assertEquals(2, individualStored.getSamples().size());
        assertTrue(individualStored.getSamples().stream().map(Sample::getId).collect(Collectors.toSet()).containsAll(Arrays.asList(5L,
                7L)));

        // Update samples
        ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), individual.getSamples());
        Individual update = catalogIndividualDBAdaptor.update(individualStored.getId(), params, QueryOptions.empty()).first();
        assertEquals(2, update.getSamples().size());
        assertTrue(update.getSamples().stream().map(Sample::getId).collect(Collectors.toSet()).containsAll(Arrays.asList(5L, 7L)));
    }

    @Test
    public void testSeveralAnnotationsInUniqueVariableSet() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        Set<Variable> variableSet = new HashSet<>();
        variableSet.add(new Variable().setName("key").setType(Variable.VariableType.TEXT));
        variableSet.add(new Variable().setName("key2").setType(Variable.VariableType.TEXT));
        variableSet.add(new Variable().setName("key3").setType(Variable.VariableType.INTEGER));
        variableSet.add(new Variable().setName("key4").setType(Variable.VariableType.BOOLEAN));
        VariableSet vs = new VariableSet().setId(3L).setVariables(variableSet).setUnique(true);

        Map<String, Object> annotationSet = new HashMap<>();
        annotationSet.put("key", "value");
        annotationSet.put("key2", "value2");
        annotationSet.put("key3", 3);
        annotationSet.put("key4", true);

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", 1, Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", 1, Collections.emptyMap());
        catalogIndividualDBAdaptor.createAnnotationSet(individualId, vs, annot1);
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("unique VariableSet");
        catalogIndividualDBAdaptor.createAnnotationSet(individualId, vs, annot2);
    }

    @Test
    public void testAnnotateIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        Set<Variable> variableSet = new HashSet<>();
        variableSet.add(new Variable().setName("key").setType(Variable.VariableType.TEXT));
        variableSet.add(new Variable().setName("key2").setType(Variable.VariableType.TEXT));
        variableSet.add(new Variable().setName("key3").setType(Variable.VariableType.INTEGER));
        variableSet.add(new Variable().setName("key4").setType(Variable.VariableType.BOOLEAN));
        VariableSet vs = new VariableSet().setId(3L).setVariables(variableSet);

        Map<String, Object> annotationSet = new HashMap<>();
        annotationSet.put("key", "value");
        annotationSet.put("key2", "value2");
        annotationSet.put("key3", 3);
        annotationSet.put("key4", true);

        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotationSet, "", 1, Collections.emptyMap());
        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotationSet, "", 1, Collections.emptyMap());
        catalogIndividualDBAdaptor.createAnnotationSet(individualId, vs, annot1);
        catalogIndividualDBAdaptor.createAnnotationSet(individualId, vs, annot2);

        Individual individual = catalogIndividualDBAdaptor.get(individualId,
                new QueryOptions(QueryOptions.INCLUDE, Constants.VARIABLE_SET + "." + 3)).first();
        Map<String, AnnotationSet> annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap
                (AnnotationSet::getName, Function.identity()));
        assertEquals(2, annotationSets.size());

        // TODO: Remove setCreationDate("") when we completely remove creationDate from the data model
        assertEquals(annot1, annotationSets.get(annot1.getName()).setCreationDate(""));
        assertEquals(annot2, annotationSets.get(annot2.getName()).setCreationDate(""));

        catalogIndividualDBAdaptor.deleteAnnotationSet(individualId, annot1.getName());

        individual = catalogIndividualDBAdaptor.get(individualId, new QueryOptions()).first();
        annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
        assertEquals(1, annotationSets.size());
        assertFalse(annotationSets.containsKey(annot1.getName()));
        // TODO: Remove setCreationDate("") when we completely remove creationDate from the data model
        assertEquals(annot2, annotationSets.get(annot2.getName()).setCreationDate(""));
    }

    @Test
    public void testAnnotateIndividualExistingAnnotationId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        Set<Variable> variableSet = new HashSet<>();
        variableSet.add(new Variable().setName("key").setType(Variable.VariableType.TEXT));
        VariableSet vs = new VariableSet().setId(3L).setVariables(variableSet);

        catalogIndividualDBAdaptor.createAnnotationSet(individualId, vs,
                new AnnotationSet("annot1", 3, new ObjectMap("key", "hello"), "", 1, Collections.emptyMap()));
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("already exists");
        catalogIndividualDBAdaptor.createAnnotationSet(individualId, vs,
                new AnnotationSet("annot1", 3, new ObjectMap("key", "hello"), "", 1, Collections.emptyMap()));
    }

    @Test
    public void testDeleteIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();
        long individualId2 = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in2", 0, individualId, "", Individual.Sex.UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getId();

        catalogIndividualDBAdaptor.delete(individualId2, new QueryOptions());
        catalogIndividualDBAdaptor.delete(individualId, new QueryOptions());
//        catalogIndividualDBAdaptor.deleteIndividual(individualId2, null);
//        catalogIndividualDBAdaptor.deleteIndividual(individualId, null);

    }

    @Test
    public void testDeleteIndividualInUse() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Individual mother = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex
                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first();
        catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in2", null, mother, null, Individual.Sex.UNKNOWN, null, "", null, null,
                null, "", null, true, 1, Collections.emptyList(), null), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.delete(mother.getId(), new QueryOptions());

    }

    @Test
    public void testDeleteIndividualInUseAsSample() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(0, "in1", 0, 0, "", Individual.Sex.UNKNOWN, "", null, 1, Collections.emptyList(), null)
                        .setSamples(Arrays.asList(new Sample().setId(5))), null).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.delete(individualId, new QueryOptions());

    }

    @Test
    public void testGetStudyIdByIndividualId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual(), null).first().getId();
        long studyIdByIndividualId = catalogIndividualDBAdaptor.getStudyId(individualId);
        assertEquals(studyId, studyIdByIndividualId);
    }

    @Test
    public void testNativeGet() throws Exception {
        long studyId = user4.getProjects().get(0).getStudies().get(0).getId();
        Individual individual = new Individual(0, "An Individual", -1, -1, "Family", Individual.Sex.MALE, "", new Individual
                .Population(), 1, Collections.emptyList(), null);
        individual = catalogIndividualDBAdaptor.insert(studyId, individual, null).first();
        Individual individual2 = new Individual(0, "Another Individual", -1, -1, "Family2", Individual.Sex.FEMALE, "", new Individual
                .Population(), 1, Collections.emptyList(), null);
        individual2 = catalogIndividualDBAdaptor.insert(studyId, individual2, null).first();
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