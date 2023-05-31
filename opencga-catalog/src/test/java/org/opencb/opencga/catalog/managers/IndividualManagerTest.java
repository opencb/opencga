package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualQualityControl;
import org.opencb.opencga.core.models.individual.IndividualReferenceParam;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class IndividualManagerTest extends AbstractManagerTest {

    @Test
    public void testAnnotateIndividual() throws CatalogException {
        VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs", null, token).first();

        String individualId1 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_1")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, token).first().getId();
        String individualId2 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_2")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, token).first().getId();
        String individualId3 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_3")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, token).first().getId();

        catalogManager.getIndividualManager().update(studyFqn, individualId1, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_1").append("AGE", 5).append("PHEN", "CASE").append("ALIVE", true)))),
                QueryOptions.empty(), token);

        catalogManager.getIndividualManager().update(studyFqn, individualId2, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_2").append("AGE", 15).append("PHEN", "CONTROL").append("ALIVE", true)))),
                QueryOptions.empty(), token);

        catalogManager.getIndividualManager().update(studyFqn, individualId3, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_3").append("AGE", 25).append("PHEN", "CASE").append("ALIVE", true)))),
                QueryOptions.empty(), token);

        List<String> individuals;
        individuals = catalogManager.getIndividualManager().search(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                        variableSet.getId() + ":NAME=~^INDIVIDUAL_"), null, token)
                .getResults().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_1", "INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getIndividualManager().search(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                        variableSet.getId() + ":AGE>10"), null, token)
                .getResults().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getIndividualManager().search(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(),
                        variableSet.getId() + ":AGE>10;" + variableSet.getId() + ":PHEN=CASE"), null, token)
                .getResults().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_3")));
    }

    @Test
    public void testDistinctDisorders() throws CatalogException {
        Individual individual = new Individual()
                .setId("i1")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder1").setName(null)));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        individual = new Individual()
                .setId("i2")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder2")));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        individual = new Individual()
                .setId("i3")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder2")));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        individual = new Individual()
                .setId("i4")
                .setDisorders(Collections.singletonList(new Disorder().setId("adisorder2")));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        OpenCGAResult<?> result = catalogManager.getIndividualManager().distinct(studyFqn,
                IndividualDBAdaptor.QueryParams.DISORDERS_ID.key(), new Query(), token);
        assertEquals(3, result.getNumResults());

        result = catalogManager.getIndividualManager().distinct(studyFqn, IndividualDBAdaptor.QueryParams.DISORDERS_ID.key(),
                new Query(IndividualDBAdaptor.QueryParams.DISORDERS.key(), "~^disor"), token);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getIndividualManager().distinct(studyFqn,
                Arrays.asList(IndividualDBAdaptor.QueryParams.DISORDERS_ID.key(), IndividualDBAdaptor.QueryParams.DISORDERS_NAME.key()),
                new Query(), token);
        assertEquals(4, result.getNumResults());
        assertFalse(result.getResults().contains(null));
    }

    @Test
    public void testSearchDisordersWithCommas() throws CatalogException {
        Individual individual = new Individual()
                .setId("i1")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder1, description 1")));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        individual = new Individual()
                .setId("i2")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder2, description 2")));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        individual = new Individual()
                .setId("i3")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder2")));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        OpenCGAResult<?> result = catalogManager.getIndividualManager().distinct(studyFqn,
                IndividualDBAdaptor.QueryParams.DISORDERS_ID.key(), new Query(), token);
        assertEquals(3, result.getNumResults());

        OpenCGAResult<Individual> search = catalogManager.getIndividualManager().search(studyFqn,
                new Query(IndividualDBAdaptor.QueryParams.DISORDERS.key(), "disorder1, description 1,disorder2, description 2"),
                IndividualManager.INCLUDE_INDIVIDUAL_IDS, token);
        assertEquals(2, search.getNumResults());
        assertTrue(Arrays.asList("i1", "i2").containsAll(search.getResults().stream().map(Individual::getId).collect(Collectors.toList())));
    }

    @Test
    public void testUpdatePhenotypes() throws CatalogException {
        Individual individual = new Individual().setId("i1");
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        List<Phenotype> phenotypeList = Arrays.asList(
                new Phenotype("phenotype0", "phenotypeName0", "SOURCE"),
                new Phenotype("phenotype1", "phenotypeName1", "SOURCE"),
                new Phenotype("phenotype2", "phenotypeName2", "SOURCE")
        );
        IndividualUpdateParams updateParams = new IndividualUpdateParams().setPhenotypes(phenotypeList);

        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, QueryOptions.empty(), token);
        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(3, individual.getPhenotypes().size());
        for (int i = 0; i < individual.getPhenotypes().size(); i++) {
            assertEquals("phenotype" + i, individual.getPhenotypes().get(i).getId());
        }

        // ACTION REMOVE phenotype0, phenotype2
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(IndividualDBAdaptor.QueryParams.PHENOTYPES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        updateParams = new IndividualUpdateParams().setPhenotypes(Arrays.asList(
                new Phenotype("phenotype0", "phenotypeName0", "SOURCE"), new Phenotype("phenotype2", "phenotypeName2", "SOURCE")));
        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, options, token);
        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(1, individual.getPhenotypes().size());
        assertEquals("phenotype1", individual.getPhenotypes().get(0).getId());

        // ADD phenotype1, phenotype2
        actionMap.put(IndividualDBAdaptor.QueryParams.PHENOTYPES.key(), ParamUtils.BasicUpdateAction.ADD);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        updateParams = new IndividualUpdateParams().setPhenotypes(Arrays.asList(
                new Phenotype("phenotype1", "phenotypeName1", "SOURCE"), new Phenotype("phenotype2", "phenotypeName2", "SOURCE")));
        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, options, token);
        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(2, individual.getPhenotypes().size());
        for (int i = 0; i < individual.getPhenotypes().size(); i++) {
            assertEquals("phenotype" + (i + 1), individual.getPhenotypes().get(i).getId());
        }

        // SET phenotype2, phenotype3
        actionMap.put(IndividualDBAdaptor.QueryParams.PHENOTYPES.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        phenotypeList = Arrays.asList(
                new Phenotype("phenotype2", "phenotypeName2", "SOURCE"),
                new Phenotype("phenotype3", "phenotypeName3", "SOURCE")
        );
        updateParams = new IndividualUpdateParams().setPhenotypes(phenotypeList);
        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, options, token);
        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(2, individual.getPhenotypes().size());
        for (int i = 0; i < individual.getPhenotypes().size(); i++) {
            assertEquals("phenotype" + (i + 2), individual.getPhenotypes().get(i).getId());
        }
    }

    @Test
    public void testUpdateDisorders() throws CatalogException {
        Individual individual = new Individual().setId("i1");
        catalogManager.getIndividualManager().create(studyFqn, individual, null, token);

        List<Disorder> disorderList = Arrays.asList(
                new Disorder("disorder0", "disorderName0", "SOURCE", null, "", null),
                new Disorder("disorder1", "disorderName1", "SOURCE", null, "", null),
                new Disorder("disorder2", "disorderName2", "SOURCE", null, "", null)
        );
        IndividualUpdateParams updateParams = new IndividualUpdateParams().setDisorders(disorderList);

        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, QueryOptions.empty(), token);
        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(3, individual.getDisorders().size());
        for (int i = 0; i < individual.getDisorders().size(); i++) {
            assertEquals("disorder" + i, individual.getDisorders().get(i).getId());
        }

        // ACTION REMOVE phenotype0, phenotype2
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(IndividualDBAdaptor.QueryParams.DISORDERS.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        updateParams = new IndividualUpdateParams().setDisorders(Arrays.asList(
                new Disorder("disorder0", "disorderName0", "SOURCE", null, "", null), new Disorder("disorder2", "disorder2", "SOURCE",
                        null, "", null)));
        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, options, token);
        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(1, individual.getDisorders().size());
        assertEquals("disorder1", individual.getDisorders().get(0).getId());

        // ADD phenotype1, phenotype2
        actionMap.put(IndividualDBAdaptor.QueryParams.DISORDERS.key(), ParamUtils.BasicUpdateAction.ADD);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        updateParams = new IndividualUpdateParams().setDisorders(Arrays.asList(
                new Disorder("disorder1", "disorderName1", "SOURCE", null, "", null), new Disorder("disorder2", "disorderName2", "SOURCE",
                        null, "", null)));
        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, options, token);
        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(2, individual.getDisorders().size());
        for (int i = 0; i < individual.getDisorders().size(); i++) {
            assertEquals("disorder" + (i + 1), individual.getDisorders().get(i).getId());
        }

        // SET phenotype2, phenotype3
        actionMap.put(IndividualDBAdaptor.QueryParams.DISORDERS.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        disorderList = Arrays.asList(
                new Disorder("disorder2", "disorderName2", "SOURCE", null, "", null),
                new Disorder("disorder3", "disorderName3", "SOURCE", null, "", null)
        );
        updateParams = new IndividualUpdateParams().setDisorders(disorderList);
        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, options, token);
        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(2, individual.getDisorders().size());
        for (int i = 0; i < individual.getDisorders().size(); i++) {
            assertEquals("disorder" + (i + 2), individual.getDisorders().get(i).getId());
        }
    }

    @Test
    public void testUpdateIndividualSamples() throws CatalogException {
        Sample sample = new Sample().setId("sample1");
        Sample sample2 = new Sample().setId("sample2");
        Sample sample3 = new Sample().setId("sample3");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);
        catalogManager.getSampleManager().create(studyFqn, sample2, QueryOptions.empty(), token);
        catalogManager.getSampleManager().create(studyFqn, sample3, QueryOptions.empty(), token);

        Individual individual = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("individual"),
                Arrays.asList(sample.getId(), sample2.getId()), INCLUDE_RESULT, token).first();
        assertEquals(2, individual.getSamples().size());
        assertEquals(2, individual.getSamples().stream().map(Sample::getId)
                .filter(s -> Arrays.asList(sample.getId(), sample2.getId()).contains(s)).count());

        // Increase sample2 version
        catalogManager.getSampleManager().update(studyFqn, sample2.getId(), new SampleUpdateParams().setDescription("new description"),
                new QueryOptions(), token);

        // Add sample2 (with new version) and sample3
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        IndividualUpdateParams updateParams = new IndividualUpdateParams().setSamples(Arrays.asList(
                new SampleReferenceParam().setId(sample2.getId()), new SampleReferenceParam().setId(sample3.getId())));
        OpenCGAResult<Individual> update = catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams,
                options, token);
        assertEquals(1, update.getNumUpdated());

        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(3, individual.getSamples().size());
        assertEquals(3, individual.getSamples().stream().map(Sample::getId)
                .filter(s -> Arrays.asList(sample.getId(), sample2.getId(), sample3.getId()).contains(s)).count());
        // Sample1 and sample3 should have version 2
        assertEquals(2, individual.getSamples().stream().map(Sample::getVersion)
                .filter(s -> s == 2).count());
        // And sample2 should be in version 3
        assertEquals(1, individual.getSamples().stream().map(Sample::getVersion)
                .filter(s -> s == 3).count());

        // Remove Sample2
        actionMap.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        updateParams =
                new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sample2.getId())));
        update = catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, options, token);
        assertEquals(1, update.getNumUpdated());

        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(2, individual.getSamples().size());
        assertEquals(2, individual.getSamples().stream().map(Sample::getId)
                .filter(s -> Arrays.asList(sample.getId(), sample3.getId()).contains(s)).count());

        // Set sample and sample2
        actionMap.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        updateParams = new IndividualUpdateParams().setSamples(Arrays.asList(
                new SampleReferenceParam().setId(sample.getId()), new SampleReferenceParam().setId(sample2.getId())));
        update = catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, options, token);
        assertEquals(1, update.getNumUpdated());

        individual = catalogManager.getIndividualManager().get(studyFqn, individual.getId(), QueryOptions.empty(), token).first();
        assertEquals(2, individual.getSamples().size());
        assertEquals(2, individual.getSamples().stream().map(Sample::getId)
                .filter(s -> Arrays.asList(sample.getId(), sample2.getId()).contains(s)).count());
    }

    @Test
    public void changeIndividualIdTest() throws CatalogException {
        Sample sample1 = new Sample().setId("sample1");
        Sample sample2 = new Sample().setId("sample2");
        Sample sample3 = new Sample().setId("sample3");
        catalogManager.getSampleManager().create(studyFqn, sample1, QueryOptions.empty(), token);
        catalogManager.getSampleManager().create(studyFqn, sample2, QueryOptions.empty(), token);
        catalogManager.getSampleManager().create(studyFqn, sample3, QueryOptions.empty(), token);
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("individual1"),
                Arrays.asList(sample1.getId(), sample2.getId(), sample3.getId()), QueryOptions.empty(), token);
        List<Sample> samples = catalogManager.getSampleManager().get(studyFqn,
                Arrays.asList(sample1.getId(), sample2.getId(), sample3.getId()), QueryOptions.empty(), token).getResults();
        for (Sample sample : samples) {
            assertEquals("individual1", sample.getIndividualId());
        }

        Sample sample4 = new Sample().setId("sample4");
        Sample sample5 = new Sample().setId("sample5");
        Sample sample6 = new Sample().setId("sample6");
        catalogManager.getSampleManager().create(studyFqn, sample4, QueryOptions.empty(), token);
        catalogManager.getSampleManager().create(studyFqn, sample5, QueryOptions.empty(), token);
        catalogManager.getSampleManager().create(studyFqn, sample6, QueryOptions.empty(), token);

        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("individual2"),
                Arrays.asList(sample4.getId(), sample5.getId(), sample6.getId()), QueryOptions.empty(), token);
        samples = catalogManager.getSampleManager().get(studyFqn,
                Arrays.asList(sample4.getId(), sample5.getId(), sample6.getId()), QueryOptions.empty(), token).getResults();
        for (Sample sample : samples) {
            assertEquals("individual2", sample.getIndividualId());
        }

        // Update individual id
        catalogManager.getIndividualManager().update(studyFqn, "individual1", new IndividualUpdateParams().setId("newId1"),
                QueryOptions.empty(), token);
        assertEquals(1, catalogManager.getIndividualManager().get(studyFqn, "newId1", QueryOptions.empty(), token).getNumResults());

        catalogManager.getIndividualManager().update(studyFqn, "individual2", new IndividualUpdateParams().setId("newId2"),
                QueryOptions.empty(), token);
        assertEquals(1, catalogManager.getIndividualManager().get(studyFqn, "newId2", QueryOptions.empty(), token).getNumResults());

        samples = catalogManager.getSampleManager().get(studyFqn,
                Arrays.asList(sample1.getId(), sample2.getId(), sample3.getId(), sample4.getId(), sample5.getId(), sample6.getId()),
                QueryOptions.empty(), token).getResults();
        for (Sample sample : samples) {
            switch (sample.getId()) {
                case "sample1":
                case "sample2":
                case "sample3":
                    assertEquals("newId1", sample.getIndividualId());
                    break;
                case "sample4":
                case "sample5":
                case "sample6":
                    assertEquals("newId2", sample.getIndividualId());
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void changeIndividualIdTest2() throws CatalogException {
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("father"), null, QueryOptions.empty(), token);
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("mother"), null, QueryOptions.empty(), token);
        catalogManager.getIndividualManager().create(studyFqn, new Individual()
                        .setId("child")
                        .setFather(new Individual().setId("father"))
                        .setMother(new Individual().setId("mother")),
                null, QueryOptions.empty(), token);
        Family family = catalogManager.getFamilyManager().create(studyFqn, new Family().setId("family"),
                Arrays.asList("child", "mother", "father"), INCLUDE_RESULT, token).first();

        assertNotNull(family.getRoles());
        assertFalse(family.getRoles().isEmpty());

        assertTrue(family.getRoles().containsKey("child"));
        for (Map.Entry<String, Map<String, Family.FamiliarRelationship>> entry : family.getRoles().entrySet()) {
            if (!entry.getKey().equals("child")) {
                assertTrue(entry.getValue().containsKey("child"));
            }
        }

        // Update child's id
        catalogManager.getIndividualManager().update(studyFqn, "child", new IndividualUpdateParams().setId("newId1"),
                QueryOptions.empty(), token);
        family = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();

        assertTrue(family.getRoles().containsKey("newId1"));
        for (Map.Entry<String, Map<String, Family.FamiliarRelationship>> entry : family.getRoles().entrySet()) {
            if (!entry.getKey().equals("newId1")) {
                assertTrue(entry.getValue().containsKey("newId1"));
            }
        }
    }

    @Test
    public void testUpdateWithLockedClinicalAnalysis() throws CatalogException {
        Sample sample = new Sample().setId("sample1");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        sample = new Sample().setId("sample2");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        sample = new Sample().setId("sample3");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        sample = new Sample().setId("sample4");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        Individual individual = new Individual()
                .setId("proband")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder")));
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample1", "sample2"), QueryOptions.empty(),
                token);

        individual = new Individual().setId("father");
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample3"), QueryOptions.empty(), token);

        individual = new Individual().setId("brother");
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample4"), QueryOptions.empty(), token);

        Family family = new Family().setId("family");
        catalogManager.getFamilyManager().create(studyFqn, family, Arrays.asList("proband", "father", "brother"), QueryOptions.empty(),
                token);

        family.setMembers(Arrays.asList(
                new Individual().setId("proband").setSamples(Collections.singletonList(new Sample().setId("sample2"))),
                new Individual().setId("father").setSamples(Collections.singletonList(new Sample().setId("sample3")))
        ));

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("clinical")
                .setProband(new Individual().setId("proband"))
                .setFamily(family)
                .setType(ClinicalAnalysis.Type.FAMILY);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), token);

        // We will create another clinical analysis with the same information. In this test, we will not lock clinical2
        clinicalAnalysis = new ClinicalAnalysis()
                .setId("clinical2")
                .setProband(new Individual().setId("proband"))
                .setFamily(family)
                .setType(ClinicalAnalysis.Type.FAMILY);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), token);

        // Update brother not used in Clinical Analysis
        catalogManager.getIndividualManager().update(studyFqn, "brother", new IndividualUpdateParams()
                .setName(RandomStringUtils.random(10)), new QueryOptions(), token);

        Individual individualResult = catalogManager.getIndividualManager().get(studyFqn, "brother", QueryOptions.empty(), token).first();
        assertEquals(3, individualResult.getVersion());

        Family familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(2, familyResult.getVersion());
        assertEquals(3, familyResult.getMembers().size());
        assertEquals(2, familyResult.getMembers().get(0).getVersion());
        assertEquals(2, familyResult.getMembers().get(1).getVersion());
        assertEquals(3, familyResult.getMembers().get(2).getVersion());

        ClinicalAnalysis clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(),
                token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(2, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(2, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version

        // Update father
        catalogManager.getIndividualManager().update(studyFqn, "father", new IndividualUpdateParams()
                .setName(RandomStringUtils.random(10)), new QueryOptions(), token);

        individualResult = catalogManager.getIndividualManager().get(studyFqn, "father", QueryOptions.empty(), token).first();
        assertEquals(3, individualResult.getVersion());

        familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(3, familyResult.getVersion());
        assertEquals(3, familyResult.getMembers().size());
        assertEquals(2, familyResult.getMembers().get(0).getVersion());
        assertEquals(3, familyResult.getMembers().get(1).getVersion());
        assertEquals(3, familyResult.getMembers().get(2).getVersion());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(3, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(3, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version

        // LOCK CLINICAL ANALYSIS
        catalogManager.getClinicalAnalysisManager().update(studyFqn, "clinical", new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), token);
        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertTrue(clinicalResult.isLocked());

        // Update proband
        catalogManager.getIndividualManager().update(studyFqn, "proband", new IndividualUpdateParams()
                .setName(RandomStringUtils.random(10)), new QueryOptions(), token);

        individualResult = catalogManager.getIndividualManager().get(studyFqn, "proband", QueryOptions.empty(), token).first();
        assertEquals(3, individualResult.getVersion());

        familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(4, familyResult.getVersion());
        assertEquals(3, familyResult.getMembers().size());
        assertEquals(3, familyResult.getMembers().get(0).getVersion());
        assertEquals(3, familyResult.getMembers().get(1).getVersion());
        assertEquals(3, familyResult.getMembers().get(2).getVersion());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(3, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(3, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample2 version
        assertEquals(4, clinicalResult.getFamily().getVersion());
        assertEquals(3, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(3, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getSamples().get(0).getVersion());   // proband sample2 version

        // Update father
        catalogManager.getIndividualManager().update(studyFqn, "father", new IndividualUpdateParams()
                .setName(RandomStringUtils.random(10)), new QueryOptions(), token);

        individualResult = catalogManager.getIndividualManager().get(studyFqn, "father", QueryOptions.empty(), token).first();
        assertEquals(4, individualResult.getVersion());

        familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(5, familyResult.getVersion());
        assertEquals(3, familyResult.getMembers().size());
        assertEquals(3, familyResult.getMembers().get(0).getVersion());
        assertEquals(4, familyResult.getMembers().get(1).getVersion());
        assertEquals(3, familyResult.getMembers().get(2).getVersion());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(3, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(3, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample2 version
        assertEquals(5, clinicalResult.getFamily().getVersion());
        assertEquals(3, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(4, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getSamples().get(0).getVersion());   // proband sample2 version
    }

    @Test
    public void testUpdateIndividualQualityControl() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        DataResult<Individual> individualDataResult = individualManager.create(studyFqn, new Individual().setId("Test")
                .setDateOfBirth("19870214"), INCLUDE_RESULT, token);

        IndividualQualityControl qualityControl = new IndividualQualityControl(null, null, null, Collections.emptyList(),
                Arrays.asList(new ClinicalComment("pfurio", "message", Collections.singletonList("tag"), "today")));
        DataResult<Individual> update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setQualityControl(qualityControl), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());

        Individual individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals(1, individual.getQualityControl().getComments().size());
        assertEquals("pfurio", individual.getQualityControl().getComments().get(0).getAuthor());
        assertEquals("message", individual.getQualityControl().getComments().get(0).getMessage());
        assertEquals("tag", individual.getQualityControl().getComments().get(0).getTags().get(0));
        assertEquals("today", individual.getQualityControl().getComments().get(0).getDate());
    }

    @Test
    public void testUpdateIndividualInfo() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        DataResult<Individual> individualDataResult = individualManager.create(studyFqn, new Individual().setId("Test")
                .setDateOfBirth("19870214"), INCLUDE_RESULT, token);
        assertEquals(1, individualDataResult.getNumResults());
        assertEquals("Test", individualDataResult.first().getId());
        assertEquals("19870214", individualDataResult.first().getDateOfBirth());

        DataResult<Individual> update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setDateOfBirth(""), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());

        Individual individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals("", individual.getDateOfBirth());

        update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setDateOfBirth("19870214"), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());
        individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals("19870214", individual.getDateOfBirth());

        update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setAttributes(Collections.singletonMap("key", "value")), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());
        individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals("value", individual.getAttributes().get("key"));

        update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setAttributes(Collections.singletonMap("key2", "value2")), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());
        individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals("value", individual.getAttributes().get("key")); // Keep "key"
        assertEquals("value2", individual.getAttributes().get("key2")); // add new "key2"

        // Wrong date of birth format
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Invalid date of birth format");
        individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setDateOfBirth("198421"), QueryOptions.empty(), token);
    }

    @Test
    public void testUpdateIndividualParents() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        individualManager.create(studyFqn, new Individual().setId("child"), QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("father"), QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("mother"), QueryOptions.empty(), token);

        DataResult<Individual> individualDataResult = individualManager.update(studyFqn, "child",
                new IndividualUpdateParams().setFather(new IndividualReferenceParam("father", ""))
                        .setMother(new IndividualReferenceParam("mother", "")), QueryOptions.empty(), token);
        assertEquals(1, individualDataResult.getNumUpdated());

        Individual individual = individualManager.get(studyFqn, "child", QueryOptions.empty(), token).first();

        assertEquals("mother", individual.getMother().getId());
        assertEquals(1, individual.getMother().getVersion());

        assertEquals("father", individual.getFather().getId());
        assertEquals(1, individual.getFather().getVersion());
    }

    @Test
    public void testRemoveIndividualParents() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        individualManager.create(studyFqn, new Individual().setId("child"), QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("father"), QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("mother"), QueryOptions.empty(), token);

        DataResult<Individual> individualDataResult = individualManager.update(studyFqn, "child",
                new IndividualUpdateParams().setFather(new IndividualReferenceParam("father", ""))
                        .setMother(new IndividualReferenceParam("mother", "")), QueryOptions.empty(), token);
        assertEquals(1, individualDataResult.getNumUpdated());

        Individual individual = individualManager.get(studyFqn, "child", QueryOptions.empty(), token).first();

        assertEquals("mother", individual.getMother().getId());
        assertEquals(1, individual.getMother().getVersion());

        assertEquals("father", individual.getFather().getId());
        assertEquals(1, individual.getFather().getVersion());

        individualManager.update(studyFqn, "child", new IndividualUpdateParams().setFather(new IndividualReferenceParam("", ""))
                .setMother(new IndividualReferenceParam("", "")), QueryOptions.empty(), token);
        individual = individualManager.get(studyFqn, "child", QueryOptions.empty(), token).first();

        assertNull(individual.getMother().getId());
        assertNull(individual.getFather().getId());
    }

    @Test
    public void testIndividualRelatives() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        individualManager.create(studyFqn, new Individual().setId("proband").setSex(SexOntologyTermAnnotation.initMale()),
                QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("brother").setSex(SexOntologyTermAnnotation.initMale()),
                QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("sister").setSex(SexOntologyTermAnnotation.initFemale()),
                QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("father").setSex(SexOntologyTermAnnotation.initMale()),
                QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("mother").setSex(SexOntologyTermAnnotation.initFemale()),
                QueryOptions.empty(), token);

        individualManager.update(studyFqn, "proband", new IndividualUpdateParams().setFather(new IndividualReferenceParam("father", ""))
                .setMother(new IndividualReferenceParam("mother", "")), QueryOptions.empty(), token);
        individualManager.update(studyFqn, "brother", new IndividualUpdateParams().setFather(new IndividualReferenceParam("father", ""))
                .setMother(new IndividualReferenceParam("mother", "")), QueryOptions.empty(), token);
        individualManager.update(studyFqn, "sister", new IndividualUpdateParams().setFather(new IndividualReferenceParam("father", ""))
                .setMother(new IndividualReferenceParam("mother", "")), QueryOptions.empty(), token);

        OpenCGAResult<Individual> relatives = catalogManager.getIndividualManager().relatives(studyFqn, "proband", 2,
                new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key()), token);

        assertEquals(5, relatives.getNumResults());
        for (Individual individual : relatives.getResults()) {
            assertEquals(individual.getId().toUpperCase(),
                    ((ObjectMap) individual.getAttributes().get("OPENCGA_RELATIVE")).getString("RELATION"));
        }
    }

    @Test
    public void testDeleteIndividualWithFamilies() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        Individual child = individualManager.create(studyFqn, new Individual()
                        .setId("child")
                        .setPhenotypes(Collections.singletonList(new Phenotype().setId("phenotype1")))
                        .setDisorders(Collections.singletonList(new Disorder().setId("disorder1"))),
                INCLUDE_RESULT, token).first();
        Individual father = new Individual()
                .setId("father")
                .setPhenotypes(Collections.singletonList(new Phenotype().setId("phenotype2")))
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder2")));
        Individual mother = new Individual()
                .setId("mother")
                .setPhenotypes(Collections.singletonList(new Phenotype().setId("phenotype3")))
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder3")));

        FamilyManager familyManager = catalogManager.getFamilyManager();
        familyManager.create(studyFqn, new Family().setId("family1").setMembers(Collections.singletonList(father)),
                Collections.singletonList(child.getId()), QueryOptions.empty(), token);
        familyManager.create(studyFqn, new Family().setId("family2").setMembers(Collections.singletonList(mother)),
                Arrays.asList(father.getId(), child.getId()), QueryOptions.empty(), token);

        try {
            DataResult writeResult = individualManager.delete(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ID.key(), "child"),
                    new QueryOptions(), token);
            fail("Expected fail");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("found in the families"));
        }

        DataResult writeResult = individualManager.delete(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ID.key(), "child"),
                new QueryOptions(Constants.FORCE, true), token);
        assertEquals(1, writeResult.getNumDeleted());

        Family family1 = familyManager.get(studyFqn, "family1", QueryOptions.empty(), token).first();
        Family family2 = familyManager.get(studyFqn, "family2", QueryOptions.empty(), token).first();

        assertEquals(1, family1.getMembers().size());
        assertEquals(0, family1.getMembers().stream().filter(i -> i.getId().equals("child")).count());
        assertEquals(1, family1.getDisorders().size());
        assertEquals(0, family1.getDisorders().stream().filter(d -> d.getId().equals("disorder1")).count());
        assertEquals(1, family1.getPhenotypes().size());
        assertEquals(0, family1.getPhenotypes().stream().filter(d -> d.getId().equals("phenotype1")).count());

        assertEquals(2, family2.getMembers().size());
        assertEquals(0, family2.getMembers().stream().filter(i -> i.getId().equals("child")).count());
        assertEquals(2, family2.getDisorders().size());
        assertEquals(0, family2.getDisorders().stream().filter(d -> d.getId().equals("disorder1")).count());
        assertEquals(2, family2.getPhenotypes().size());
        assertEquals(0, family2.getPhenotypes().stream().filter(d -> d.getId().equals("phenotype1")).count());

        System.out.println(writeResult.getTime());
    }

    @Test
    public void testGetIndividualWithSamples() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        individualManager.create(studyFqn, new Individual().setId("individual1")
                        .setSamples(Arrays.asList(new Sample().setId("sample1"), new Sample().setId("sample2"), new Sample().setId(
                                "sample3"))),
                QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("individual2")
                        .setSamples(Arrays.asList(new Sample().setId("sample4"), new Sample().setId("sample5"), new Sample().setId(
                                "sample6"))),
                QueryOptions.empty(), token);

        DataResult<Individual> search = individualManager.search(studyFqn, new Query(), QueryOptions.empty(), token);
        assertEquals(2, search.getNumResults());
        search.getResults().forEach(i -> {
            assertEquals(3, i.getSamples().size());
            assertTrue(org.apache.commons.lang3.StringUtils.isNotEmpty(i.getSamples().get(0).getCreationDate()));
            if (i.getId().equals("individual1")) {
                assertTrue(Arrays.asList("sample1", "sample2", "sample3").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            } else {
                assertTrue(Arrays.asList("sample4", "sample5", "sample6").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            }
        });

        search = individualManager.search(studyFqn, new Query(), new QueryOptions(QueryOptions.EXCLUDE, "samples.creationDate"),
                token);
        assertEquals(2, search.getNumResults());
        search.getResults().forEach(i -> {
            assertEquals(3, i.getSamples().size());
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(i.getSamples().get(0).getCreationDate()));
            if (i.getId().equals("individual1")) {
                assertTrue(Arrays.asList("sample1", "sample2", "sample3").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            } else {
                assertTrue(Arrays.asList("sample4", "sample5", "sample6").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            }
        });

        search = individualManager.search(studyFqn, new Query(), new QueryOptions(QueryOptions.INCLUDE, "samples.id"),
                token);
        assertEquals(2, search.getNumResults());
        search.getResults().forEach(i -> {
            assertEquals(3, i.getSamples().size());
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(i.getSamples().get(0).getCreationDate()));
            if (i.getId().equals("individual1")) {
                assertTrue(Arrays.asList("sample1", "sample2", "sample3").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            } else {
                assertTrue(Arrays.asList("sample4", "sample5", "sample6").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            }
        });

        search = individualManager.search(studyFqn, new Query(), new QueryOptions(QueryOptions.INCLUDE, "id,creationDate,samples.id"),
                token);
        assertEquals(2, search.getNumResults());
        search.getResults().forEach(i -> {
            assertTrue(org.apache.commons.lang3.StringUtils.isNotEmpty(i.getCreationDate()));
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(i.getName()));
            assertEquals(3, i.getSamples().size());
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(i.getSamples().get(0).getCreationDate()));
            if (i.getId().equals("individual1")) {
                assertTrue(Arrays.asList("sample1", "sample2", "sample3").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            } else {
                assertTrue(Arrays.asList("sample4", "sample5", "sample6").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            }
        });
    }

    // Test versioning
    @Test
    public void incrementVersionTest() throws CatalogException {
        Individual dummyIndividual1 = DummyModelUtils.getDummyIndividual(null, null, null, null);
        Individual dummyIndividual2 = DummyModelUtils.getDummyIndividual(null, null, null, null);

        catalogManager.getIndividualManager().create(studyFqn, dummyIndividual1, QueryOptions.empty(), token);
        catalogManager.getIndividualManager().create(studyFqn, dummyIndividual2, QueryOptions.empty(), token);

        OpenCGAResult<Individual> result = catalogManager.getIndividualManager().get(studyFqn,
                Arrays.asList(dummyIndividual1.getId(), dummyIndividual2.getId()), QueryOptions.empty(), token);
        for (Individual individual : result.getResults()) {
            assertEquals(1, individual.getVersion());
        }

        catalogManager.getIndividualManager().update(studyFqn, dummyIndividual1.getId(), new IndividualUpdateParams().setName("name"),
                QueryOptions.empty(), token);
        result = catalogManager.getIndividualManager().get(studyFqn,
                Arrays.asList(dummyIndividual1.getId(), dummyIndividual2.getId()), QueryOptions.empty(), token);
        assertEquals(2, result.first().getVersion());
        assertEquals("name", result.first().getName());
        assertEquals(1, result.getResults().get(1).getVersion());

        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.ID.key(), dummyIndividual1.getId())
                .append(Constants.ALL_VERSIONS, true);

        result = catalogManager.getIndividualManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(2, result.getNumResults());
        assertEquals(dummyIndividual1.getId(), result.getResults().get(0).getName());
        assertEquals(1, result.getResults().get(0).getVersion());
        assertEquals(2, result.getResults().get(1).getVersion());
        assertEquals("name", result.getResults().get(1).getName());
    }

    // Test updates and relationships
    @Test
    public void memberReferenceTest() throws CatalogException {
        // Create individual with 2 samples
        Sample sample1 = DummyModelUtils.getDummySample();
        Sample sample2 = DummyModelUtils.getDummySample();
        Individual individual1 = DummyModelUtils.getDummyIndividual(null, Arrays.asList(sample1, sample2), null, null);
        String individualId1 = individual1.getId();

        // Create another individual with another 2 samples
        Sample sample3 = DummyModelUtils.getDummySample();
        Sample sample4 = DummyModelUtils.getDummySample();
        Individual individual2 = DummyModelUtils.getDummyIndividual(null, Arrays.asList(sample3, sample4), null, null);
        String individualId2 = individual2.getId();

        // Create family
        Family family = DummyModelUtils.getDummyFamily();
        family.setMembers(null);

        QueryOptions options = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);
        individual1 = catalogManager.getIndividualManager().create(studyFqn, individual1, options, token).first();
        assertEquals(1, individual1.getVersion());
        assertEquals(2, individual1.getSamples().size());
        assertEquals(2, individual1.getSamples().stream().map(Sample::getVersion).filter(v -> v == 1).count());
        assertEquals(2, individual1.getSamples().stream().map(Sample::getIndividualId).filter(i -> i.equals(individualId1)).count());

        individual2 = catalogManager.getIndividualManager().create(studyFqn, individual2, options, token).first();
        assertEquals(1, individual2.getVersion());
        assertEquals(2, individual2.getSamples().size());
        assertEquals(2, individual2.getSamples().stream().map(Sample::getVersion).filter(v -> v == 1).count());
        assertEquals(2, individual2.getSamples().stream().map(Sample::getIndividualId).filter(i -> i.equals(individualId2)).count());

        family = catalogManager.getFamilyManager().create(studyFqn, family, Arrays.asList(individual1.getId(), individual2.getId()),
                options, token).first();
        assertEquals(2, family.getMembers().size());
        assertEquals(1, family.getVersion());
        assertEquals(2, family.getMembers().stream().map(Individual::getVersion).filter(v -> v == 2).count());

        // Update individual 2
        individual2 = catalogManager.getIndividualManager().update(studyFqn, individual2.getId(), new IndividualUpdateParams()
                        .setName("blabla"), options, token).first();
        assertEquals(2, individual2.getSamples().size());
        assertEquals(3, individual2.getVersion());
        assertEquals(2, individual2.getSamples().stream().map(Sample::getVersion).filter(v -> v == 1).count());
        assertEquals(2, individual2.getSamples().stream().map(Sample::getIndividualId).filter(i -> i.equals(individualId2)).count());
        assertEquals(1, individual2.getFamilyIds().size());
        assertEquals(family.getId(), individual2.getFamilyIds().get(0));

        family = catalogManager.getFamilyManager().get(studyFqn, family.getId(), QueryOptions.empty(), token).first();
        assertTrue(family.getRoles().containsKey(individualId1));
        assertFalse(family.getRoles().containsKey("blabla"));
        assertEquals(2, family.getMembers().size());
        for (Individual member : family.getMembers()) {
            if (member.getId().equals(individualId1)) {
                assertEquals(2, member.getVersion());
            } else if (member.getId().equals(individualId2)) {
                assertEquals(3, member.getVersion());
            } else {
                fail("Family should not have any other individual");
            }
        }

        // Update id from individual1
        individual1 = catalogManager.getIndividualManager().update(studyFqn, individual1.getId(), new IndividualUpdateParams()
                        .setId("blabla"), options, token).first();
        assertEquals(2, individual1.getSamples().size());
        assertEquals(3, individual1.getVersion());
        assertEquals(2, individual1.getSamples().stream().map(Sample::getVersion).filter(v -> v == 2).count());
        assertEquals(2, individual1.getSamples().stream().map(Sample::getIndividualId).filter(i -> i.equals("blabla")).count());
        assertEquals(1, individual1.getFamilyIds().size());
        assertEquals(family.getId(), individual1.getFamilyIds().get(0));

        family = catalogManager.getFamilyManager().get(studyFqn, family.getId(), QueryOptions.empty(), token).first();
        assertEquals(2, family.getMembers().size());
        assertEquals(2, family.getMembers().stream().map(Individual::getVersion).filter(v -> v == 3).count());
        assertFalse(family.getRoles().containsKey(individualId1));
        assertTrue(family.getRoles().containsKey("blabla"));

        List<Sample> samples = catalogManager.getSampleManager().get(studyFqn, Arrays.asList(sample1.getId(), sample2.getId()),
                QueryOptions.empty(), token).getResults();
        for (Sample sample : samples) {
            assertEquals("blabla", sample.getIndividualId());
            assertEquals(2, sample.getVersion());
        }
    }

    // Test update when use in CA
    @Test
    public void updateInUseInCATest() throws CatalogException {
        QueryOptions options = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

        Family family = DummyModelUtils.getDummyCaseFamily("family1");
        for (int i = family.getMembers().size() - 1; i >= 0; i--) {
            catalogManager.getIndividualManager().create(studyFqn, family.getMembers().get(i), QueryOptions.empty(), token);
        }

        List<String> members = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        family.setMembers(null);
        family = catalogManager.getFamilyManager().create(studyFqn, family, members, options, token).first();

        // Unlocked cases
        ClinicalAnalysis case1 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);
        ClinicalAnalysis case2 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);

        // locked true
        ClinicalAnalysis case3 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null)
                        .setLocked(true);

        case1 = catalogManager.getClinicalAnalysisManager().create(studyFqn, case1, options, token).first();
        assertFalse(case1.isLocked());
        case2 = catalogManager.getClinicalAnalysisManager().create(studyFqn, case2, options, token).first();
        assertFalse(case2.isLocked());
        case3 = catalogManager.getClinicalAnalysisManager().create(studyFqn, case3, options, token).first();
        assertTrue(case3.isLocked());

        String oldProbandId = case3.getProband().getId();

        // Update proband information
        Individual proband = catalogManager.getIndividualManager().update(studyFqn, oldProbandId,
                new IndividualUpdateParams().setId("blabla"), options, token).first();
        assertEquals("blabla", proband.getId());
        assertEquals(3, proband.getVersion());

        family = catalogManager.getFamilyManager().get(studyFqn, family.getId(), QueryOptions.empty(), token).first();
        assertEquals(2, family.getVersion());
        assertTrue(family.getRoles().containsKey("blabla"));
        assertEquals(1, family.getMembers().stream().filter(i -> i.getId().equals("blabla")).count());
        assertEquals(1, family.getMembers().stream().filter(i -> i.getId().equals("blabla")).filter(i -> i.getVersion() == 3).count());

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().get(studyFqn,
                Arrays.asList(case1.getId(), case2.getId(), case3.getId()), QueryOptions.empty(), token);
        case1 = result.getResults().get(0);
        case2 = result.getResults().get(1);
        case3 = result.getResults().get(2);

        assertEquals("blabla", case1.getProband().getId());
        assertEquals("blabla", case2.getProband().getId());
        assertEquals(oldProbandId, case3.getProband().getId());

        assertEquals(3, case1.getProband().getVersion());
        assertEquals(3, case2.getProband().getVersion());
        assertEquals(2, case3.getProband().getVersion());

        assertEquals(2, case1.getFamily().getVersion());
        assertEquals(2, case2.getFamily().getVersion());
        assertEquals(1, case3.getFamily().getVersion());
    }

    // Test when in use in CA
    @Test
    public void updateDeleteInUseInCATest() throws CatalogException {
        QueryOptions options = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

        Family family = DummyModelUtils.getDummyCaseFamily("family1");
        for (int i = family.getMembers().size() - 1; i >= 0; i--) {
            catalogManager.getIndividualManager().create(studyFqn, family.getMembers().get(i), QueryOptions.empty(), token);
        }

        List<String> members = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        family.setMembers(null);
        family = catalogManager.getFamilyManager().create(studyFqn, family, members, options, token).first();

        // Unlocked cases
        ClinicalAnalysis case1 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);
        ClinicalAnalysis case2 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);

        // locked true
        ClinicalAnalysis case3 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null)
                .setLocked(true);

        case1 = catalogManager.getClinicalAnalysisManager().create(studyFqn, case1, options, token).first();
        assertFalse(case1.isLocked());
        case2 = catalogManager.getClinicalAnalysisManager().create(studyFqn, case2, options, token).first();
        assertFalse(case2.isLocked());
        case3 = catalogManager.getClinicalAnalysisManager().create(studyFqn, case3, options, token).first();
        assertTrue(case3.isLocked());

        // Delete proband
        try {
            catalogManager.getIndividualManager().delete(studyFqn, Collections.singletonList(case1.getProband().getId()),
                    new QueryOptions(ParamConstants.FORCE, true), token);
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("in use in 3 cases"));
        }

        // unlock case3
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case3.getId(), new ClinicalAnalysisUpdateParams().setLocked(false),
                QueryOptions.empty(), token);

        try {
            catalogManager.getIndividualManager().delete(studyFqn, Collections.singletonList(case1.getProband().getId()),
                    new QueryOptions(ParamConstants.FORCE, true), token);
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("in use in 3 cases"));
        }
    }

}
