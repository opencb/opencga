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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
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
import org.opencb.opencga.core.models.individual.Location;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleInternal;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.internal.connection.tlschannel.util.Util.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by hpccoll1 on 19/06/15.
 */
@Category(MediumTests.class)
public class IndividualMongoDBAdaptorTest extends AbstractMongoDBAdaptorTest {

    @Test
    public void testCreateIndividual() throws Exception {
        OpenCGAResult<Individual> result = catalogIndividualDBAdaptor.get(studyUid, new Query(IndividualDBAdaptor.QueryParams.ID.key(), "individual"), QueryOptions.empty(), ownerUserId);
        assertEquals(0, result.getNumResults());

        OpenCGAResult individual = catalogIndividualDBAdaptor.insert(studyUid, new Individual().setId("individual"), null, null);
        assertEquals(1, individual.getNumInserted());

        result = catalogIndividualDBAdaptor.get(studyUid, new Query(IndividualDBAdaptor.QueryParams.ID.key(), "individual"), QueryOptions.empty(), ownerUserId);
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testCreateIndividualStudyNotFound() throws Exception {
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not valid");
        catalogIndividualDBAdaptor.insert(-10, new Individual(), null, null);
    }

    @Test
    public void testCreateIndividualFatherNotFound() throws Exception {
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("in1", "in1", new Individual().setId("father").setUid(10), null,
                null, null, null, null, null, null, "", Collections.emptyList(), false, 1, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), null, null);
    }

    @Test
    public void testCreateIndividualAlreadyExists() throws Exception {
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("in1", "in1", new Individual(), new Individual(), new Location(), null, null, null, null, null, "",
                Collections.emptyList(), false, 1, Collections
                .emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), null, null);
        thrown.expect(CatalogDBException.class); //Name already exists
        thrown.expectMessage("already exists");
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("in1", "in1", new Individual(), new Individual(), new Location(), null, null, null, null, null, "",
                Collections.emptyList(), false, 1, Collections
                .emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), null, null);
    }

    @Test
    public void testGetIndividual() throws Exception {
        Individual individual = new Individual("an_individual", "An Individual", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, new IndividualPopulation(), null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null);
        catalogIndividualDBAdaptor.insert(studyUid, individual, null, null);
        Individual individual2 = catalogIndividualDBAdaptor.get(individual.getUid(), null).first();
        assertEquals(individual.getId(), individual2.getId());
    }

    @Test
    public void testGetIndividualNoExists() throws Exception {
        Individual individual = new Individual("in1", "An Individual", new Individual(), new Individual(), new Location(),
                SexOntologyTermAnnotation.initMale(), null, null, new IndividualPopulation(), null, "", Collections.emptyList(), false, 1,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null);
        catalogIndividualDBAdaptor.insert(studyUid, individual, null, null);
        catalogIndividualDBAdaptor.get(individual.getUid(), null).first();
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not exist");
        catalogIndividualDBAdaptor.get(9999, null);
    }

    @Test
    public void testGetAllIndividuals() throws Exception {
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("ind_1", "ind_1", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, new IndividualPopulation(), null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), null, null);
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("ind_2", "ind_2", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, new
                IndividualPopulation(), null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), null, null);

        catalogIndividualDBAdaptor.insert(studyUid, new Individual("ind_3", "ind_3", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, new IndividualPopulation(), null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), null, null);
        Individual father = getIndividual(studyUid, "ind_3");
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("ind_4", "ind_4", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, new IndividualPopulation(), null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), null, null);
        Individual mother = getIndividual(studyUid, "ind_4");
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("ind_5", "ind_5", father, mother, null, SexOntologyTermAnnotation.initMale(),
                IndividualProperty.KaryotypicSex.XY, null, new IndividualPopulation(), null, null, null, true, 1, Collections.emptyList(),
                null, null, IndividualInternal.init(), null), null, null);
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("ind_6", "ind_6", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, new IndividualPopulation(), null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), null, null);

        DataResult<Individual> result;
        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.ID.key(),
                "~ind_[1-3]").append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid), new QueryOptions());

        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.SEX_ID.key(),
                IndividualProperty.Sex.FEMALE).append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid), new QueryOptions());
        assertEquals(3, result.getNumResults());

        result = catalogIndividualDBAdaptor.get(new Query(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), ">0")
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid), new QueryOptions());
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testModifyIndividual() throws Exception {
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("in1", "in1", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initUnknown(), null, null, null, null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), Collections.emptyList(), null);
        long individualUid = getIndividual(studyUid, "in1").getUid();

        ObjectMap params = new ObjectMap("sex", SexOntologyTermAnnotation.initMale());
        catalogIndividualDBAdaptor.update(individualUid, params, QueryOptions.empty());
        Individual individual = catalogIndividualDBAdaptor.get(individualUid, null).first();
        assertEquals(IndividualProperty.Sex.MALE, individual.getSex().getSex());
    }

    @Test
    public void testModifyIndividualBadFatherId() throws Exception {
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("in1", "in1", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initUnknown(), null, null, null, null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), Collections.emptyList(), null);
        long individualUid = getIndividual(studyUid, "in1").getUid();

        thrown.expect(CatalogDBException.class);
        catalogIndividualDBAdaptor.update(individualUid, new ObjectMap(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), 4000),
                QueryOptions.empty());
    }

    @Test
    public void testModifyIndividualNegativeFatherId() throws Exception {
        catalogIndividualDBAdaptor.insert(studyUid, new Individual("in1", "in1", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initUnknown(), null, null, null, null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null), Collections.emptyList(), null);
        long individualUid = getIndividual(studyUid, "in1").getUid();

        DataResult result = catalogIndividualDBAdaptor.update(individualUid,
                new ObjectMap(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), -1), QueryOptions.empty());
        assertEquals(1, result.getNumUpdated());

        Individual individual = catalogIndividualDBAdaptor.get(individualUid,
                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.FATHER.key())).first();
        assertEquals(-1, individual.getFather().getUid());
    }

    @Test
    public void testAvoidDuplicatedSamples() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, new Sample().setId("sample1").setInternal(SampleInternal.init()),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample1 = getSample(studyUid, "sample1");
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, new Sample().setId("sample2").setInternal(SampleInternal.init()),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample2 = getSample(studyUid, "sample2");

        Individual individual = new Individual()
                .setId("in2")
                .setInternal(IndividualInternal.init())
                .setSamples(Arrays.asList(sample1, sample1, sample2));
        catalogIndividualDBAdaptor.insert(studyUid, individual, Collections.emptyList(), null);
        Individual individualStored = getIndividual(studyUid, "in2");
        assertEquals(2, individualStored.getSamples().size());
        assertTrue(individualStored.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet()).containsAll(Arrays.asList(
                sample1.getUid(), sample2.getUid())));

        List<Sample> sampleList = Arrays.asList(individualStored.getSamples().get(0), individualStored.getSamples().get(1),
                individualStored.getSamples().get(0), individualStored.getSamples().get(1));

        // Update samples
        ObjectMap params = new ObjectMap(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
        DataResult result = catalogIndividualDBAdaptor.update(individualStored.getUid(), params, QueryOptions.empty());
        assertEquals(1, result.getNumUpdated());

        individual = catalogIndividualDBAdaptor.get(individualStored.getUid(),
                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key())).first();

        assertEquals(2, individual.getSamples().size());
        assertTrue(individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet()).containsAll(Arrays.asList(sample1.getUid(),
                sample2.getUid())));
        assertEquals(2, individual.getSamples().stream().map(Sample::getVersion).filter(v -> v == 2).count());
    }

//    @Test
//    public void testSeveralAnnotationsInUniqueVariableSet() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
//        Individual individual = new Individual().setId("in1").setName("in1");
//        catalogIndividualDBAdaptor.insert(studyUid, individual, null, null);
//        long individualUid = individual.getUid();
//
//        Set<Variable> variableSet = new HashSet<>();
//        variableSet.add(new Variable().setId("key").setType(Variable.VariableType.STRING));
//        variableSet.add(new Variable().setId("key2").setType(Variable.VariableType.STRING));
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
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualUid, vs, annot1);
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("unique VariableSet");
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualUid, vs, annot2);
//    }
//
//    @Test
//    public void testAnnotateIndividual() throws Exception {
//        Individual individual = new Individual().setId("in1").setName("in1");
//        catalogIndividualDBAdaptor.insert(studyUid, individual, null, null);
//        long individualUid = individual.getUid();
//
//        Set<Variable> variableSet = new HashSet<>();
//        variableSet.add(new Variable().setId("key").setType(Variable.VariableType.STRING));
//        variableSet.add(new Variable().setId("key2").setType(Variable.VariableType.STRING));
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
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualUid, vs, annot1);
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualUid, vs, annot2);
//
//        individual = catalogIndividualDBAdaptor.get(individualUid,
//                new QueryOptions(QueryOptions.INCLUDE, Constants.VARIABLE_SET + "." + 3)).first();
//        Map<String, AnnotationSet> annotationSets = individual.getAnnotationSets().stream().collect(Collectors.toMap
//                (AnnotationSet::getId, Function.identity()));
//        assertEquals(2, annotationSets.size());
//    }
//
//    @Test
//    public void testAnnotateIndividualExistingAnnotationId() throws Exception {
//        Individual individual = new Individual().setId("in1").setName("in1");
//        catalogIndividualDBAdaptor.insert(studyUid, individual, null, null);
//        long individualUid = individual.getUid();
//
//        Set<Variable> variableSet = new HashSet<>();
//        variableSet.add(new Variable().setId("key").setType(Variable.VariableType.STRING));
//        VariableSet vs = new VariableSet().setUid(3L).setVariables(variableSet).setId("vsId");
//
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualUid, vs,
//                new AnnotationSet("annot1", vs.getId(), new ObjectMap("key", "hello"), "", 1, Collections.emptyMap()));
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("already exists");
//        catalogIndividualDBAdaptor.createAnnotationSetForMigration(individualUid, vs,
//                new AnnotationSet("annot1", vs.getId(), new ObjectMap("key", "hello"), "", 1, Collections.emptyMap()));
//    }

    @Test
    public void testGetStudyIdByIndividualId() throws Exception {
        catalogIndividualDBAdaptor.insert(studyUid, new Individual().setId("individual").setInternal(IndividualInternal.init()),
                Collections.emptyList(), null);
        long individualUid = getIndividual(studyUid, "individual").getUid();
        long studyIdByIndividualId = catalogIndividualDBAdaptor.getStudyId(individualUid);
        assertEquals(studyUid, studyIdByIndividualId);
    }

    @Test
    public void testNativeGet() throws Exception {
        Individual individual = new Individual("in1", "An Individual", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, new IndividualPopulation(), null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null);
        catalogIndividualDBAdaptor.insert(studyUid, individual, Collections.emptyList(), null);
        Individual individual2 = new Individual("in2", "Another Individual", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, new IndividualPopulation(), null, "",
                Collections.emptyList(), false, 1, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), null);
        catalogIndividualDBAdaptor.insert(studyUid, individual2, Collections.emptyList(), null);
        DataResult queryResult = catalogIndividualDBAdaptor.nativeGet(new Query(IndividualDBAdaptor.QueryParams.ID.key(),
                individual.getId()), new QueryOptions());

        // Individual
        List<Document> results = queryResult.getResults();
        assertEquals(1, results.size());
        assertEquals("MALE", results.get(0).get("sex", Document.class).getString("id"));

        queryResult = catalogIndividualDBAdaptor.nativeGet(new Query(IndividualDBAdaptor.QueryParams.ID.key(), individual2.getId()),
                new QueryOptions());

        // Individual2
        results = queryResult.getResults();
        assertEquals(1, results.size());
        assertEquals("FEMALE", results.get(0).get("sex", Document.class).getString("id"));
    }
}