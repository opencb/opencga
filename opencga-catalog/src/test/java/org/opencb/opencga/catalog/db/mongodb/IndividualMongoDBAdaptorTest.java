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

package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualInternal;
import org.opencb.opencga.core.models.individual.IndividualPopulation;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.sample.SampleInternal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualMongoDBAdaptorTest extends MongoDBAdaptorTest {

    @Test
    public void testCreateIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual().setId("individual"), null, null);
    }

    @Test
    public void testCreateIndividualStudyNotFound() throws Exception {
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.insert(-10, new Individual(), null, null);
    }

    @Test
    public void testCreateIndividualFatherNotFound() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", new Individual().setId("father").setUid(10), null,
                null, null, null, "", null, null, "", Collections.emptyList(), false, 1, Collections.emptyList(),
                Collections.emptyList(), null), null, null);
    }

    @Test
    public void testCreateIndividualAlreadyExists() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", null, "", null, 1, Collections
                .emptyList(), null), null, null);
        thrown.expect(CatalogDBException.class); //Name already exists
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", null, "", null, 1, Collections
                .emptyList(), null), null, null);
    }

    @Test
    public void testGetIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        Individual individual = new Individual("an_individual", "An Individual", IndividualProperty.Sex.MALE, "",
                new IndividualPopulation(), 1, Collections.emptyList(), null);
        catalogIndividualDBAdaptor.insert(studyId, individual, null, null);
        Individual individual2 = catalogIndividualDBAdaptor.get(individual.getUid(), null).first();
        assertEquals(individual.getId(), individual2.getId());
    }

    @Test
    public void testGetIndividualNoExists() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        Individual individual = new Individual("in1", "An Individual", IndividualProperty.Sex.MALE, "", new IndividualPopulation(), 1, Collections.emptyList(), null);
        catalogIndividualDBAdaptor.insert(studyId, individual, null, null);
        catalogIndividualDBAdaptor.get(individual.getUid(), null).first();
        thrown.expect(CatalogDBException.class); //Id not found
        catalogIndividualDBAdaptor.get(9999, null);
    }

    @Test
    public void testGetAllIndividuals() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual("ind_1", "ind_1", IndividualProperty.Sex.MALE, "", 
                new IndividualPopulation(), 1, Collections.emptyList(), null), null, null);
        catalogIndividualDBAdaptor.insert(studyId, new Individual("ind_2", "ind_2", IndividualProperty.Sex.FEMALE, "", new
                IndividualPopulation(), 1, Collections.emptyList(), null), null, null);

        catalogIndividualDBAdaptor.insert(studyId, new Individual("ind_3", "ind_3", IndividualProperty.Sex.MALE, "",
                new IndividualPopulation(), 1, Collections.emptyList(), null), null, null);
        Individual father = getIndividual(studyId, "ind_3");
        catalogIndividualDBAdaptor.insert(studyId, new Individual("ind_4", "ind_4", IndividualProperty.Sex.FEMALE, "",
                new IndividualPopulation(), 1, Collections.emptyList(), null), null, null);
        Individual mother = getIndividual(studyId, "ind_4");
        catalogIndividualDBAdaptor.insert(studyId, new Individual("ind_5", "ind_5", father, mother, null, IndividualProperty.Sex.MALE,
                IndividualProperty.KaryotypicSex.XY, "", new IndividualPopulation(), null, null, null, true, 1, Collections.emptyList(),
                null, null), null, null);
        catalogIndividualDBAdaptor.insert(studyId, new Individual("ind_6", "ind_6", IndividualProperty.Sex.FEMALE, "",
                new IndividualPopulation(), 1, Collections.emptyList(), null), null, null);

        DataResult<Individual> result;
        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.ID.key(),
                "~ind_[1-3]").append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId), new QueryOptions());

        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.SEX.key(),
                IndividualProperty.Sex.FEMALE).append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId), new QueryOptions());
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), ">0")
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId), new QueryOptions());
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testModifyIndividual() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", IndividualProperty.Sex.UNKNOWN, "", null, 1,
                Collections.emptyList(), null), Collections.emptyList(), null);
        long individualUid = getIndividual(studyId, "in1").getUid();

        ObjectMap params = new ObjectMap("sex", "MALE");
        catalogIndividualDBAdaptor.update(individualUid, params, QueryOptions.empty());
        Individual individual = catalogIndividualDBAdaptor.get(individualUid, null).first();
        assertEquals(IndividualProperty.Sex.MALE, individual.getSex());
    }

    @Test
    public void testModifyIndividualBadGender() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", IndividualProperty.Sex.UNKNOWN, "", null, 1,
                Collections.emptyList(), null), Collections.emptyList(), null);
        long individualUid = getIndividual(studyId, "in1").getUid();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualUid, new ObjectMap("sex", "bad sex"), QueryOptions.empty());
    }

    @Test
    public void testModifyIndividualBadFatherId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", IndividualProperty.Sex.UNKNOWN, "", null, 1,
                Collections.emptyList(), null), Collections.emptyList(), null);
        long individualUid = getIndividual(studyId, "in1").getUid();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualUid, new ObjectMap(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), 4000),
                QueryOptions.empty());
    }

    @Test
    public void testModifyIndividualNegativeFatherId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", IndividualProperty.Sex.UNKNOWN, "", null, 1,
                Collections.emptyList(), null), Collections.emptyList(), null);
        long individualUid = getIndividual(studyId, "in1").getUid();

        DataResult result = catalogIndividualDBAdaptor.update(individualUid,
                new ObjectMap(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), -1), QueryOptions.empty());
        assertEquals(1, result.getNumUpdated());

        Individual individual = catalogIndividualDBAdaptor.get(individualUid,
                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.FATHER.key())).first();
        assertEquals(-1, individual.getFather().getUid());
    }

    // FIXME: This should be tested in the managers
    @Ignore
    @Test
    public void testModifyIndividualExistingName() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", IndividualProperty.Sex.UNKNOWN, "", null, 1,
                Collections.emptyList(), null), Collections.emptyList(), null);
        long individualUid = getIndividual(studyId, "in1").getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual("in2", "in2", IndividualProperty.Sex.UNKNOWN, "", null, 1,
                Collections.emptyList(), null), Collections.emptyList(), null);

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualUid, new ObjectMap("name", "in2"), QueryOptions.empty());
    }

    @Test
    public void testAvoidDuplicatedSamples() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        new Status();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(studyId, new Sample().setId("sample1").setInternal(new SampleInternal(new Status())),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample1 = getSample(studyId, "sample1");
        new Status();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(studyId, new Sample().setId("sample2").setInternal(new SampleInternal(new Status())),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample2 = getSample(studyId, "sample2");

        Individual individual = new Individual()
                .setId("in2")
                .setInternal(new IndividualInternal(new Status()))
                .setSamples(Arrays.asList(sample1, sample1, sample2));
        catalogIndividualDBAdaptor.insert(studyId, individual, Collections.emptyList(), null);
        Individual individualStored = getIndividual(studyId, "in2");
        assertEquals(2, individualStored.getSamples().size());
        assertTrue(individualStored.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet()).containsAll(Arrays.asList(
                sample1.getUid(), sample2.getUid())));

        // Update samples
        ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), individual.getSamples());
        DataResult result = catalogIndividualDBAdaptor.update(individualStored.getUid(), params, QueryOptions.empty());
        assertEquals(1, result.getNumUpdated());

        individual = catalogIndividualDBAdaptor.get(individualStored.getUid(),
                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key())).first();

        assertEquals(2, individual.getSamples().size());
        assertTrue(individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet()).containsAll(Arrays.asList(sample1.getUid(),
                sample2.getUid())));
    }

//    @Test
//    public void testSeveralAnnotationsInUniqueVariableSet() throws CatalogDBException {
//        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
//        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", 0, 0, "", IndividualProperty.Sex
//                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getUid();
//
//        Set<Variable> variableSet = new HashSet<>();
//        variableSet.add(new Variable().setId("key").setType(Variable.VariableType.TEXT));
//        variableSet.add(new Variable().setId("key2").setType(Variable.VariableType.TEXT));
//        variableSet.add(new Variable().setId("key3").setType(Variable.VariableType.INTEGER));
//        variableSet.add(new Variable().setId("key4").setType(Variable.VariableType.BOOLEAN));
//        VariableSet vs = new VariableSet().setUid(3L).setVariables(variableSet).setUnique(true).setId("vsId");
//
//        Map<String, Object> annotationSet = new HashMap<>();
//        annotationSet.put("key", "value");
//        annotationSet.put("key2", "value2");
//        annotationSet.put("key3", 3);
//        annotationSet.put("key4", true);
//
//        AnnotationSet annot1 = new AnnotationSet("annot1", vs.getId(), annotationSet, "", 1, Collections.emptyMap());
//        AnnotationSet annot2 = new AnnotationSet("annot2", vs.getId(), annotationSet, "", 1, Collections.emptyMap());
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualId, vs, annot1);
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("unique VariableSet");
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualId, vs, annot2);
//    }
//
//    @Test
//    public void testAnnotateIndividual() throws Exception {
//        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
//        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", 0, 0, "", IndividualProperty.Sex
//                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getUid();
//
//        Set<Variable> variableSet = new HashSet<>();
//        variableSet.add(new Variable().setId("key").setType(Variable.VariableType.TEXT));
//        variableSet.add(new Variable().setId("key2").setType(Variable.VariableType.TEXT));
//        variableSet.add(new Variable().setId("key3").setType(Variable.VariableType.INTEGER));
//        variableSet.add(new Variable().setId("key4").setType(Variable.VariableType.BOOLEAN));
//        VariableSet vs = new VariableSet().setUid(3L).setVariables(variableSet).setId("vsId");
//
//        Map<String, Object> annotationSet = new HashMap<>();
//        annotationSet.put("key", "value");
//        annotationSet.put("key2", "value2");
//        annotationSet.put("key3", 3);
//        annotationSet.put("key4", true);
//
//        AnnotationSet annot1 = new AnnotationSet("annot1", vs.getId(), annotationSet, "", 1, Collections.emptyMap());
//        AnnotationSet annot2 = new AnnotationSet("annot2", vs.getId(), annotationSet, "", 1, Collections.emptyMap());
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualId, vs, annot1);
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualId, vs, annot2);
//
//        Individual individual = catalogIndividualDBAdaptor.get(individualId,
//                new QueryOptions(QueryOptions.INCLUDE, Constants.VARIABLE_SET + "." + 3)).first();
//        Map<String, AnnotationSet> annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap
//                (AnnotationSet::getId, Function.identity()));
//        assertEquals(2, annotationSets.size());
//    }

//    @Test
//    public void testAnnotateIndividualExistingAnnotationId() throws Exception {
//        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
//        long individualId = catalogIndividualDBAdaptor.insert(studyId, new Individual("in1", "in1", 0, 0, "", IndividualProperty.Sex
//                .UNKNOWN, "", null, 1, Collections.emptyList(), null), null).first().getUid();
//
//        Set<Variable> variableSet = new HashSet<>();
//        variableSet.add(new Variable().setId("key").setType(Variable.VariableType.TEXT));
//        VariableSet vs = new VariableSet().setUid(3L).setVariables(variableSet).setId("vsId");
//
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualId, vs,
//                new AnnotationSet("annot1", vs.getId(), new ObjectMap("key", "hello"), "", 1, Collections.emptyMap()));
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("already exists");
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualId, vs,
//                new AnnotationSet("annot1", vs.getId(), new ObjectMap("key", "hello"), "", 1, Collections.emptyMap()));
//    }

    @Test
    public void testGetStudyIdByIndividualId() throws Exception {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        catalogIndividualDBAdaptor.insert(studyId, new Individual().setId("individual").setInternal(new IndividualInternal(new Status())),
                Collections.emptyList(), null);
        long individualUid = getIndividual(studyId, "individual").getUid();
        long studyIdByIndividualId = catalogIndividualDBAdaptor.getStudyId(individualUid);
        assertEquals(studyId, studyIdByIndividualId);
    }

    @Test
    public void testNativeGet() throws Exception {
        long studyId = user4.getProjects().get(0).getStudies().get(0).getUid();
        Individual individual = new Individual("in1", "An Individual", IndividualProperty.Sex.MALE, "", new IndividualPopulation(), 1,
                Collections.emptyList(), null);
        catalogIndividualDBAdaptor.insert(studyId, individual, Collections.emptyList(), null);
        Individual individual2 = new Individual("in2", "Another Individual", IndividualProperty.Sex.FEMALE, "", new IndividualPopulation(),
                1, Collections.emptyList(), null);
        catalogIndividualDBAdaptor.insert(studyId, individual2, Collections.emptyList(), null);
        DataResult queryResult = catalogIndividualDBAdaptor.nativeGet(new Query(IndividualDBAdaptor.QueryParams.ID.key(),
                individual.getId()), new QueryOptions());

        // Individual
        List<Document> results = queryResult.getResults();
        assertEquals(1, results.size());
        assertEquals("MALE", results.get(0).get("sex"));

        queryResult = catalogIndividualDBAdaptor.nativeGet(new Query(IndividualDBAdaptor.QueryParams.ID.key(), individual2.getId()),
                new QueryOptions());

        // Individual2
        results = queryResult.getResults();
        assertEquals(1, results.size());
        assertEquals("FEMALE", results.get(0).get("sex"));
    }
}