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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisUpdateParams;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyAclParams;
import org.opencb.opencga.core.models.family.FamilyQualityControl;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclParams;
import org.opencb.opencga.core.models.individual.IndividualReferenceParam;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.commons.datastore.mongodb.MongoDBQueryUtils.Accumulator.max;

/**
 * Created by pfurio on 11/05/17.
 */
@Category(MediumTests.class)
public class FamilyManagerTest extends AbstractManagerTest {

    private FamilyManager familyManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        familyManager = catalogManager.getFamilyManager();
    }

    @Test
    public void createFamily() throws CatalogException {
        DataResult<Family> familyDataResult = createDummyFamily("Martinez-Martinez", true);

        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(5, familyDataResult.first().getMembers().size());
        assertEquals(2, familyDataResult.first().getPhenotypes().size());
        assertEquals(5, familyDataResult.first().getRoles().size());

        boolean motherIdUpdated = false;
        boolean fatherIdUpdated = false;
        for (Individual relatives : familyDataResult.first().getMembers()) {
            if (relatives.getMother().getUid() > 0) {
                motherIdUpdated = true;
            }
            if (relatives.getFather().getUid() > 0) {
                fatherIdUpdated = true;
            }
        }

        assertTrue("Mother id not associated to any children", motherIdUpdated);
        assertTrue("Father id not associated to any children", fatherIdUpdated);

        // Create family again with individuals already created
        familyDataResult = createDummyFamily("Other-Family-Name", false);

        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(5, familyDataResult.first().getMembers().size());
        assertEquals(2, familyDataResult.first().getPhenotypes().size());
        assertEquals(5, familyDataResult.first().getRoles().size());

        motherIdUpdated = false;
        fatherIdUpdated = false;
        for (Individual relatives : familyDataResult.first().getMembers()) {
            if (relatives.getMother().getUid() > 0) {
                motherIdUpdated = true;
            }
            if (relatives.getFather().getUid() > 0) {
                fatherIdUpdated = true;
            }
        }

        assertTrue("Mother id not associated to any children", motherIdUpdated);
        assertTrue("Father id not associated to any children", fatherIdUpdated);
    }

    @Test
    public void deleteFamilyTest() throws CatalogException {
        DataResult<Family> familyDataResult = createDummyFamily("Martinez-Martinez", true);
        String familyId = familyDataResult.first().getId();

        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(5, familyDataResult.first().getMembers().size());
        for (Individual member : familyDataResult.first().getMembers()) {
            assertEquals(1, member.getFamilyIds().size());
            assertEquals(familyId, member.getFamilyIds().get(0));
        }

        catalogManager.getFamilyManager().delete(studyFqn, Collections.singletonList(familyId), QueryOptions.empty(), ownerToken);
        try {
            catalogManager.getFamilyManager().get(studyFqn, familyId, QueryOptions.empty(), ownerToken);
            fail("Family should not exist");
        } catch (CatalogException e) {
            // empty block
        }

        List<String> members = familyDataResult.first().getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        OpenCGAResult<Individual> result = catalogManager.getIndividualManager().get(studyFqn, members, QueryOptions.empty(), ownerToken);

        for (Individual member : result.getResults()) {
            assertTrue(member.getFamilyIds().isEmpty());
        }
    }

    @Test
    public void deleteWithClinicalAnalysisTest() throws CatalogException {
        Sample sample = new Sample().setId("sample1");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), ownerToken);

        sample = new Sample().setId("sample2");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), ownerToken);

        sample = new Sample().setId("sample3");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), ownerToken);

        sample = new Sample().setId("sample4");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), ownerToken);

        Individual individual = new Individual()
                .setId("proband")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder")));
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample1", "sample2"), QueryOptions.empty(),
                ownerToken);

        individual = new Individual().setId("father");
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample3"), QueryOptions.empty(), ownerToken);

        Family family = new Family().setId("family");
        catalogManager.getFamilyManager().create(studyFqn, family, Arrays.asList("proband", "father"), QueryOptions.empty(), ownerToken);

        family.setMembers(Arrays.asList(
                new Individual().setId("proband").setSamples(Collections.singletonList(new Sample().setId("sample2"))),
                new Individual().setId("father").setSamples(Collections.singletonList(new Sample().setId("sample3")))
        ));

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("clinical")
                .setProband(new Individual().setId("proband"))
                .setFamily(family)
                .setLocked(true)
                .setType(ClinicalAnalysis.Type.FAMILY);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);

        try {
            catalogManager.getFamilyManager().delete(studyFqn, Collections.singletonList(family.getId()), QueryOptions.empty(), ownerToken);
            fail("Clinical is locked. It should  not delete anything");
        } catch (CatalogException e) {
            System.out.println(e.getMessage());
            // empty block
        }

        OpenCGAResult<?> result = catalogManager.getFamilyManager().get(studyFqn, family.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams()
                        .setLocked(false),
                QueryOptions.empty(), ownerToken);

        try {
            catalogManager.getFamilyManager().delete(studyFqn, Collections.singletonList(family.getId()), QueryOptions.empty(), ownerToken);
            fail("Clinical is not locked. It should  not delete anything either");
        } catch (CatalogException e) {
            System.out.println(e.getMessage());
            // empty block
        }
    }

    @Test
    public void updateFamilyReferencesInIndividualTest() throws CatalogException {
        DataResult<Family> familyDataResult = createDummyFamily("Martinez-Martinez", true);
        for (Individual member : familyDataResult.first().getMembers()) {
            assertEquals(1, member.getFamilyIds().size());
            assertEquals(familyDataResult.first().getId(), member.getFamilyIds().get(0));
        }

        // Create a new individual
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("john"), QueryOptions.empty(), ownerToken);
        FamilyUpdateParams updateParams = new FamilyUpdateParams()
                .setMembers(Arrays.asList(
                        new IndividualReferenceParam().setId("john"),
                        new IndividualReferenceParam().setId("father"),
                        new IndividualReferenceParam().setId("mother"),
                        new IndividualReferenceParam().setId("child1")
                ));

        familyManager.update(studyFqn, familyDataResult.first().getId(), updateParams, QueryOptions.empty(), ownerToken);
        Family family = familyManager.get(studyFqn, familyDataResult.first().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(4, family.getMembers().size());
        assertTrue(Arrays.asList("john", "father", "mother", "child1")
                .containsAll(family.getMembers().stream().map(Individual::getId).collect(Collectors.toList())));
        for (Individual member : familyDataResult.first().getMembers()) {
            assertEquals(1, member.getFamilyIds().size());
            assertEquals(familyDataResult.first().getId(), member.getFamilyIds().get(0));
        }

        // Check removed members no longer belong to the family
        List<Individual> individualList = catalogManager.getIndividualManager().get(studyFqn, Arrays.asList("child2", "child3"),
                QueryOptions.empty(), ownerToken).getResults();
        assertEquals(2, individualList.size());
        for (Individual individual : individualList) {
            assertEquals(0, individual.getFamilyIds().size());
        }

        createDummyFamily("Other-Family-Name", false);
        individualList = catalogManager.getIndividualManager().get(studyFqn, Arrays.asList("john", "father", "mother", "child1", "child2",
                "child3"), QueryOptions.empty(), ownerToken).getResults();
        for (Individual individual : individualList) {
            switch (individual.getId()) {
                case "john":
                    assertEquals(1, individual.getFamilyIds().size());
                    assertEquals("Martinez-Martinez", individual.getFamilyIds().get(0));
                    break;
                case "father":
                case "mother":
                case "child1":
                    assertEquals(2, individual.getFamilyIds().size());
                    assertTrue(individual.getFamilyIds().containsAll(Arrays.asList("Martinez-Martinez", "Other-Family-Name")));
                    break;
                case "child2":
                case "child3":
                    assertEquals(1, individual.getFamilyIds().size());
                    assertEquals("Other-Family-Name", individual.getFamilyIds().get(0));
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void createComplexFamily() throws CatalogException {
        Individual paternalGrandfather = new Individual().setId("p_grandfather");
        Individual paternalGrandmother = new Individual().setId("p_grandmother");
        Individual maternalGrandfather = new Individual().setId("m_grandfather");
        Individual maternalGrandmother = new Individual().setId("m_grandmother");
        Individual father = new Individual().setId("father").setFather(paternalGrandfather).setMother(paternalGrandmother);
        Individual mother = new Individual().setId("mother").setMother(maternalGrandmother).setFather(maternalGrandfather);
        Individual proband = new Individual().setId("proband").setFather(father).setMother(mother);
        Individual brother = new Individual().setId("brother")
                .setFather(father).setMother(mother).setSex(SexOntologyTermAnnotation.initMale());
        Individual sister = new Individual().setId("sister")
                .setFather(father).setMother(mother).setSex(SexOntologyTermAnnotation.initFemale());
        Individual sibling = new Individual().setId("sibling").setFather(father).setMother(mother);

        catalogManager.getFamilyManager().create(studyFqn, new Family().setId("family").setMembers(
                Arrays.asList(paternalGrandfather, paternalGrandmother, maternalGrandfather, maternalGrandmother, mother, father, proband,
                        brother, sister, sibling)), QueryOptions.empty(), ownerToken);
        OpenCGAResult<Family> family = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), ownerToken);
        Map<String, Map<String, Family.FamiliarRelationship>> roles = family.first().getRoles();
        assertEquals(10, family.first().getMembers().size());

        Map<String, Family.FamiliarRelationship> pGrandfather = roles.get("p_grandfather");
        assertEquals(5, pGrandfather.size());
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, pGrandfather.get("father"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, pGrandfather.get("proband"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, pGrandfather.get("sibling"));
        assertEquals(Family.FamiliarRelationship.GRANDSON, pGrandfather.get("brother"));
        assertEquals(Family.FamiliarRelationship.GRANDDAUGHTER, pGrandfather.get("sister"));

        Map<String, Family.FamiliarRelationship> pGrandmother = roles.get("p_grandmother");
        assertEquals(5, pGrandmother.size());
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, pGrandmother.get("father"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, pGrandmother.get("proband"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, pGrandmother.get("sibling"));
        assertEquals(Family.FamiliarRelationship.GRANDSON, pGrandmother.get("brother"));
        assertEquals(Family.FamiliarRelationship.GRANDDAUGHTER, pGrandmother.get("sister"));

        Map<String, Family.FamiliarRelationship> mGrandfather = roles.get("m_grandfather");
        assertEquals(5, mGrandfather.size());
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, mGrandfather.get("mother"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, mGrandfather.get("proband"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, mGrandfather.get("sibling"));
        assertEquals(Family.FamiliarRelationship.GRANDSON, mGrandfather.get("brother"));
        assertEquals(Family.FamiliarRelationship.GRANDDAUGHTER, mGrandfather.get("sister"));

        Map<String, Family.FamiliarRelationship> mGrandmother = roles.get("m_grandmother");
        assertEquals(5, mGrandmother.size());
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, mGrandmother.get("mother"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, mGrandmother.get("proband"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, mGrandmother.get("sibling"));
        assertEquals(Family.FamiliarRelationship.GRANDSON, mGrandmother.get("brother"));
        assertEquals(Family.FamiliarRelationship.GRANDDAUGHTER, mGrandmother.get("sister"));

        Map<String, Family.FamiliarRelationship> motherMap = roles.get("mother");
        assertEquals(6, motherMap.size());
        assertEquals(Family.FamiliarRelationship.MOTHER, motherMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.FATHER, motherMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, motherMap.get("proband"));
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, motherMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.SON, motherMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.DAUGHTER, motherMap.get("sister"));

        Map<String, Family.FamiliarRelationship> fatherMap = roles.get("father");
        assertEquals(6, fatherMap.size());
        assertEquals(Family.FamiliarRelationship.MOTHER, fatherMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.FATHER, fatherMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, fatherMap.get("proband"));
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, fatherMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.SON, fatherMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.DAUGHTER, fatherMap.get("sister"));

        Map<String, Family.FamiliarRelationship> probandMap = roles.get("proband");
        assertEquals(9, probandMap.size());
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDMOTHER, probandMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDFATHER, probandMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDMOTHER, probandMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDFATHER, probandMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.MOTHER, probandMap.get("mother"));
        assertEquals(Family.FamiliarRelationship.FATHER, probandMap.get("father"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, probandMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.BROTHER, probandMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.SISTER, probandMap.get("sister"));

        Map<String, Family.FamiliarRelationship> siblingMap = roles.get("sibling");
        assertEquals(9, siblingMap.size());
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDMOTHER, siblingMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDFATHER, siblingMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDMOTHER, siblingMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDFATHER, siblingMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.MOTHER, siblingMap.get("mother"));
        assertEquals(Family.FamiliarRelationship.FATHER, siblingMap.get("father"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, siblingMap.get("proband"));
        assertEquals(Family.FamiliarRelationship.BROTHER, siblingMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.SISTER, siblingMap.get("sister"));

        Map<String, Family.FamiliarRelationship> brotherMap = roles.get("brother");
        assertEquals(9, brotherMap.size());
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDMOTHER, brotherMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDFATHER, brotherMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDMOTHER, brotherMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDFATHER, brotherMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.MOTHER, brotherMap.get("mother"));
        assertEquals(Family.FamiliarRelationship.FATHER, brotherMap.get("father"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, brotherMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, brotherMap.get("proband"));
        assertEquals(Family.FamiliarRelationship.SISTER, brotherMap.get("sister"));

        Map<String, Family.FamiliarRelationship> sisterMap = roles.get("sister");
        assertEquals(9, sisterMap.size());
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDMOTHER, sisterMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDFATHER, sisterMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDMOTHER, sisterMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDFATHER, sisterMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.MOTHER, sisterMap.get("mother"));
        assertEquals(Family.FamiliarRelationship.FATHER, sisterMap.get("father"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, sisterMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.BROTHER, sisterMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, sisterMap.get("proband"));
    }

    @Test
    public void updateFamilyRoles() throws CatalogException {
        Individual paternalGrandfather = new Individual().setId("p_grandfather");
        Individual paternalGrandmother = new Individual().setId("p_grandmother");
        Individual maternalGrandfather = new Individual().setId("m_grandfather");
        Individual maternalGrandmother = new Individual().setId("m_grandmother");
        Individual father = new Individual().setId("father").setSex(SexOntologyTermAnnotation.initMale());
        Individual mother = new Individual().setId("mother").setSex(SexOntologyTermAnnotation.initFemale());
        Individual proband = new Individual().setId("proband");
        Individual brother = new Individual().setId("brother").setSex(SexOntologyTermAnnotation.initMale());
        Individual sister = new Individual().setId("sister").setSex(SexOntologyTermAnnotation.initFemale());
        Individual sibling = new Individual().setId("sibling");

        catalogManager.getFamilyManager().create(studyFqn, new Family().setId("family").setMembers(
                Arrays.asList(paternalGrandfather, paternalGrandmother, maternalGrandfather, maternalGrandmother, mother, father, proband,
                        brother, sister, sibling)), QueryOptions.empty(), ownerToken);
        OpenCGAResult<Family> family = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), ownerToken);
        Map<String, Map<String, Family.FamiliarRelationship>> roles = family.first().getRoles();
        assertEquals(10, family.first().getMembers().size());
        for (Map.Entry<String, Map<String, Family.FamiliarRelationship>> entry : family.first().getRoles().entrySet()) {
            assertEquals(0, entry.getValue().size());
        }
        // We perform all individual updates to set which are the individual's parents
        IndividualUpdateParams updateParams = new IndividualUpdateParams()
                .setFather(new IndividualReferenceParam().setId(paternalGrandfather.getId()))
                .setMother(new IndividualReferenceParam().setId(paternalGrandmother.getId()));
        catalogManager.getIndividualManager().update(studyFqn, father.getId(), updateParams, QueryOptions.empty(), ownerToken);

        updateParams = new IndividualUpdateParams()
                .setFather(new IndividualReferenceParam().setId(maternalGrandfather.getId()))
                .setMother(new IndividualReferenceParam().setId(maternalGrandmother.getId()));
        catalogManager.getIndividualManager().update(studyFqn, mother.getId(), updateParams, QueryOptions.empty(), ownerToken);

        updateParams = new IndividualUpdateParams()
                .setFather(new IndividualReferenceParam().setId(father.getId()))
                .setMother(new IndividualReferenceParam().setId(mother.getId()));
        catalogManager.getIndividualManager().update(studyFqn, proband.getId(), updateParams, QueryOptions.empty(), ownerToken);
        catalogManager.getIndividualManager().update(studyFqn, brother.getId(), updateParams, QueryOptions.empty(), ownerToken);
        catalogManager.getIndividualManager().update(studyFqn, sister.getId(), updateParams, QueryOptions.empty(), ownerToken);
        catalogManager.getIndividualManager().update(studyFqn, sibling.getId(), updateParams, QueryOptions.empty(), ownerToken);

//        catalogManager.getFamilyManager().update(studyFqn, family.first().getId(), null,
//        new QueryOptions(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true), token);

        // Roles should have been automatically updated containing up to date roles
        family = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), ownerToken);
        roles = family.first().getRoles();
        assertEquals(10, family.first().getMembers().size());
        Map<String, Family.FamiliarRelationship> pGrandfather = roles.get("p_grandfather");
        assertEquals(5, pGrandfather.size());
        assertEquals(Family.FamiliarRelationship.SON, pGrandfather.get("father"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, pGrandfather.get("proband"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, pGrandfather.get("sibling"));
        assertEquals(Family.FamiliarRelationship.GRANDSON, pGrandfather.get("brother"));
        assertEquals(Family.FamiliarRelationship.GRANDDAUGHTER, pGrandfather.get("sister"));

        Map<String, Family.FamiliarRelationship> pGrandmother = roles.get("p_grandmother");
        assertEquals(5, pGrandmother.size());
        assertEquals(Family.FamiliarRelationship.SON, pGrandmother.get("father"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, pGrandmother.get("proband"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, pGrandmother.get("sibling"));
        assertEquals(Family.FamiliarRelationship.GRANDSON, pGrandmother.get("brother"));
        assertEquals(Family.FamiliarRelationship.GRANDDAUGHTER, pGrandmother.get("sister"));

        Map<String, Family.FamiliarRelationship> mGrandfather = roles.get("m_grandfather");
        assertEquals(5, mGrandfather.size());
        assertEquals(Family.FamiliarRelationship.DAUGHTER, mGrandfather.get("mother"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, mGrandfather.get("proband"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, mGrandfather.get("sibling"));
        assertEquals(Family.FamiliarRelationship.GRANDSON, mGrandfather.get("brother"));
        assertEquals(Family.FamiliarRelationship.GRANDDAUGHTER, mGrandfather.get("sister"));

        Map<String, Family.FamiliarRelationship> mGrandmother = roles.get("m_grandmother");
        assertEquals(5, mGrandmother.size());
        assertEquals(Family.FamiliarRelationship.DAUGHTER, mGrandmother.get("mother"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, mGrandmother.get("proband"));
        assertEquals(Family.FamiliarRelationship.GRANDCHILD, mGrandmother.get("sibling"));
        assertEquals(Family.FamiliarRelationship.GRANDSON, mGrandmother.get("brother"));
        assertEquals(Family.FamiliarRelationship.GRANDDAUGHTER, mGrandmother.get("sister"));

        Map<String, Family.FamiliarRelationship> motherMap = roles.get("mother");
        assertEquals(6, motherMap.size());
        assertEquals(Family.FamiliarRelationship.MOTHER, motherMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.FATHER, motherMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, motherMap.get("proband"));
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, motherMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.SON, motherMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.DAUGHTER, motherMap.get("sister"));

        Map<String, Family.FamiliarRelationship> fatherMap = roles.get("father");
        assertEquals(6, fatherMap.size());
        assertEquals(Family.FamiliarRelationship.MOTHER, fatherMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.FATHER, fatherMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, fatherMap.get("proband"));
        assertEquals(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, fatherMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.SON, fatherMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.DAUGHTER, fatherMap.get("sister"));

        Map<String, Family.FamiliarRelationship> probandMap = roles.get("proband");
        assertEquals(9, probandMap.size());
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDMOTHER, probandMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDFATHER, probandMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDMOTHER, probandMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDFATHER, probandMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.MOTHER, probandMap.get("mother"));
        assertEquals(Family.FamiliarRelationship.FATHER, probandMap.get("father"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, probandMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.BROTHER, probandMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.SISTER, probandMap.get("sister"));

        Map<String, Family.FamiliarRelationship> siblingMap = roles.get("sibling");
        assertEquals(9, siblingMap.size());
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDMOTHER, siblingMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDFATHER, siblingMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDMOTHER, siblingMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDFATHER, siblingMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.MOTHER, siblingMap.get("mother"));
        assertEquals(Family.FamiliarRelationship.FATHER, siblingMap.get("father"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, siblingMap.get("proband"));
        assertEquals(Family.FamiliarRelationship.BROTHER, siblingMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.SISTER, siblingMap.get("sister"));

        Map<String, Family.FamiliarRelationship> brotherMap = roles.get("brother");
        assertEquals(9, brotherMap.size());
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDMOTHER, brotherMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDFATHER, brotherMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDMOTHER, brotherMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDFATHER, brotherMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.MOTHER, brotherMap.get("mother"));
        assertEquals(Family.FamiliarRelationship.FATHER, brotherMap.get("father"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, brotherMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, brotherMap.get("proband"));
        assertEquals(Family.FamiliarRelationship.SISTER, brotherMap.get("sister"));

        Map<String, Family.FamiliarRelationship> sisterMap = roles.get("sister");
        assertEquals(9, sisterMap.size());
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDMOTHER, sisterMap.get("m_grandmother"));
        assertEquals(Family.FamiliarRelationship.MATERNAL_GRANDFATHER, sisterMap.get("m_grandfather"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDMOTHER, sisterMap.get("p_grandmother"));
        assertEquals(Family.FamiliarRelationship.PATERNAL_GRANDFATHER, sisterMap.get("p_grandfather"));
        assertEquals(Family.FamiliarRelationship.MOTHER, sisterMap.get("mother"));
        assertEquals(Family.FamiliarRelationship.FATHER, sisterMap.get("father"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, sisterMap.get("sibling"));
        assertEquals(Family.FamiliarRelationship.BROTHER, sisterMap.get("brother"));
        assertEquals(Family.FamiliarRelationship.FULL_SIBLING, sisterMap.get("proband"));
    }

    @Test
    public void searchFamily() throws CatalogException {
        createDummyFamily("Martinez-Martinez", true);
        createDummyFamily("Martinez", false);
        createDummyFamily("Furio", false);

        OpenCGAResult<Family> search = catalogManager.getFamilyManager().search(studyFqn,
                new Query(FamilyDBAdaptor.QueryParams.ID.key(), "~^Mart"), QueryOptions.empty(), ownerToken);
        assertEquals(2, search.getNumResults());
        for (Family result : search.getResults()) {
            assertTrue(result.getId().startsWith("Mart"));
        }
    }

    @Test
    public void testRoles() throws CatalogException {
        DataResult<Family> dummyFamily = createDummyFamily("Martinez-Martinez", true);

        assertEquals(Family.FamiliarRelationship.SON, dummyFamily.first().getRoles().get("mother").get("child1"));
        assertEquals(Family.FamiliarRelationship.DAUGHTER, dummyFamily.first().getRoles().get("mother").get("child2"));
        assertEquals(Family.FamiliarRelationship.DAUGHTER, dummyFamily.first().getRoles().get("mother").get("child3"));

        assertEquals(Family.FamiliarRelationship.SON, dummyFamily.first().getRoles().get("father").get("child1"));
        assertEquals(Family.FamiliarRelationship.DAUGHTER, dummyFamily.first().getRoles().get("father").get("child2"));
        assertEquals(Family.FamiliarRelationship.DAUGHTER, dummyFamily.first().getRoles().get("father").get("child3"));

        assertEquals(Family.FamiliarRelationship.SISTER, dummyFamily.first().getRoles().get("child1").get("child2"));
        assertEquals(Family.FamiliarRelationship.SISTER, dummyFamily.first().getRoles().get("child1").get("child3"));
        assertEquals(Family.FamiliarRelationship.FATHER, dummyFamily.first().getRoles().get("child1").get("father"));
        assertEquals(Family.FamiliarRelationship.MOTHER, dummyFamily.first().getRoles().get("child1").get("mother"));

        assertEquals(Family.FamiliarRelationship.SISTER, dummyFamily.first().getRoles().get("child3").get("child2"));
        assertEquals(Family.FamiliarRelationship.BROTHER, dummyFamily.first().getRoles().get("child3").get("child1"));
        assertEquals(Family.FamiliarRelationship.FATHER, dummyFamily.first().getRoles().get("child3").get("father"));
        assertEquals(Family.FamiliarRelationship.MOTHER, dummyFamily.first().getRoles().get("child3").get("mother"));

        assertEquals(Family.FamiliarRelationship.BROTHER, dummyFamily.first().getRoles().get("child2").get("child1"));
        assertEquals(Family.FamiliarRelationship.SISTER, dummyFamily.first().getRoles().get("child2").get("child3"));
        assertEquals(Family.FamiliarRelationship.FATHER, dummyFamily.first().getRoles().get("child2").get("father"));
        assertEquals(Family.FamiliarRelationship.MOTHER, dummyFamily.first().getRoles().get("child2").get("mother"));
    }

    @Test
    public void testPropagateFamilyPermission() throws CatalogException {
        createDummyFamily("Martinez-Martinez", true);

        catalogManager.getUserManager().create("user2", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId, null,
                opencgaToken);
        String token = catalogManager.getUserManager().login(organizationId, "user2", TestParamConstants.PASSWORD).getToken();

        try {
            familyManager.get(studyFqn, "Martinez-Martinez", QueryOptions.empty(), token);
            fail("Expected authorization exception. user2 should not be able to see the study");
        } catch (CatalogAuthorizationException ignored) {
        }

        familyManager.updateAcl(studyFqn, new FamilyAclParams("VIEW", "Martinez-Martinez", null, null, FamilyAclParams.Propagate.NO), "user2",
                ParamUtils.AclAction.SET, ownerToken);
        DataResult<Family> familyDataResult = familyManager.get(studyFqn, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(0, familyDataResult.first().getMembers().size());
        int nsamples = 0;
        for (Individual member : familyDataResult.first().getMembers()) {
            nsamples += member.getSamples().size();
        }
        assertEquals(0, nsamples);

        familyManager.updateAcl(studyFqn, new FamilyAclParams("VIEW", "Martinez-Martinez", null, null, FamilyAclParams.Propagate.YES), "user2",
                ParamUtils.AclAction.SET, ownerToken);
        familyDataResult = familyManager.get(studyFqn, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(5, familyDataResult.first().getMembers().size());
        List<Sample> sampleList = new ArrayList<>(3);
        for (Individual member : familyDataResult.first().getMembers()) {
            sampleList.addAll(member.getSamples());
        }
        assertEquals(3, sampleList.size());

        OpenCGAResult<AclEntryList<SamplePermissions>> acls = catalogManager.getSampleManager().getAcls(studyFqn,
                sampleList.stream().map(Sample::getId).collect(Collectors.toList()), "user2", false, token);
        for (AclEntryList<SamplePermissions> result : acls.getResults()) {
            assertTrue(result.getAcl().get(0).getPermissions().contains(SamplePermissions.VIEW));
            assertFalse(result.getAcl().get(0).getPermissions().contains(SamplePermissions.VIEW_VARIANTS));
        }

        familyManager.updateAcl(studyFqn, new FamilyAclParams("VIEW", "Martinez-Martinez", null, null,
                FamilyAclParams.Propagate.YES_AND_VARIANT_VIEW), "user2", ParamUtils.AclAction.SET, ownerToken);
        familyDataResult = familyManager.get(studyFqn, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(5, familyDataResult.first().getMembers().size());
        sampleList = new ArrayList<>(3);
        for (Individual member : familyDataResult.first().getMembers()) {
            sampleList.addAll(member.getSamples());
        }
        assertEquals(3, sampleList.size());

        acls = catalogManager.getSampleManager().getAcls(studyFqn, sampleList.stream().map(Sample::getId).collect(Collectors.toList()),
                "user2", false, token);
        for (AclEntryList<SamplePermissions> result : acls.getResults()) {
            assertTrue(result.getAcl().get(0).getPermissions().contains(SamplePermissions.VIEW));
            assertTrue(result.getAcl().get(0).getPermissions().contains(SamplePermissions.VIEW_VARIANTS));
        }
    }

    @Test
    public void getFamilyWithOnlyAllowedMembers2() throws CatalogException, IOException {
        createDummyFamily("Martinez-Martinez", true);

        catalogManager.getUserManager().create("user2", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId, null,
                opencgaToken);
        String token = catalogManager.getUserManager().login(organizationId, "user2", TestParamConstants.PASSWORD).getToken();

        try {
            familyManager.get(studyFqn, "Martinez-Martinez", QueryOptions.empty(), token);
            fail("Expected authorization exception. user2 should not be able to see the study");
        } catch (CatalogAuthorizationException ignored) {
        }

        familyManager.updateAcl(studyFqn, new FamilyAclParams("VIEW", "Martinez-Martinez", null, null, FamilyAclParams.Propagate.NO), "user2",
                ParamUtils.AclAction.SET, ownerToken);
        DataResult<Family> familyDataResult = familyManager.get(studyFqn, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(0, familyDataResult.first().getMembers().size());

        catalogManager.getIndividualManager().updateAcl(studyFqn, Collections.singletonList("child2"), "user2",
                new IndividualAclParams("", "VIEW"), ParamUtils.AclAction.SET, false, ownerToken);
        familyDataResult = familyManager.get(studyFqn, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(1, familyDataResult.first().getMembers().size());
        assertEquals("child2", familyDataResult.first().getMembers().get(0).getId());

        catalogManager.getIndividualManager().updateAcl(studyFqn, Collections.singletonList("child3"), "user2",
                new IndividualAclParams("", "VIEW"), ParamUtils.AclAction.SET, false, ownerToken);
        familyDataResult = familyManager.get(studyFqn, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(2, familyDataResult.first().getMembers().size());
        assertEquals("child2", familyDataResult.first().getMembers().get(0).getId());
        assertEquals("child3", familyDataResult.first().getMembers().get(1).getId());

        familyDataResult = familyManager.get(studyFqn, "Martinez-Martinez",
                new QueryOptions(QueryOptions.EXCLUDE, FamilyDBAdaptor.QueryParams.MEMBERS.key()), token);
        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(null, familyDataResult.first().getMembers());

        familyDataResult = familyManager.get(studyFqn, "Martinez-Martinez",
                new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.MEMBERS.key() + "."
                        + IndividualDBAdaptor.QueryParams.NAME.key()),
                token);
        assertEquals(1, familyDataResult.getNumResults());
        assertEquals(null, familyDataResult.first().getName());
        assertEquals(2, familyDataResult.first().getMembers().size());
        assertEquals(null, familyDataResult.first().getMembers().get(0).getId());
        assertEquals(null, familyDataResult.first().getMembers().get(1).getId());
        assertEquals("child2", familyDataResult.first().getMembers().get(0).getName());
        assertEquals("child3", familyDataResult.first().getMembers().get(1).getName());
    }

    @Test
    public void testUpdateWithLockedClinicalAnalysis() throws CatalogException {
        Sample sample = new Sample().setId("sample1");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), ownerToken);

        sample = new Sample().setId("sample2");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), ownerToken);

        sample = new Sample().setId("sample3");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), ownerToken);

        sample = new Sample().setId("sample4");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), ownerToken);

        Individual individual = new Individual()
                .setId("proband")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder")));
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample1", "sample2"), QueryOptions.empty(),
                ownerToken);

        individual = new Individual().setId("father");
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample3"), QueryOptions.empty(), ownerToken);

        Family family = new Family().setId("family");
        catalogManager.getFamilyManager().create(studyFqn, family, Arrays.asList("proband", "father"), QueryOptions.empty(), ownerToken);

        family.setMembers(Arrays.asList(
                new Individual().setId("proband").setSamples(Collections.singletonList(new Sample().setId("sample2"))),
                new Individual().setId("father").setSamples(Collections.singletonList(new Sample().setId("sample3")))
        ));

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("clinical")
                .setProband(new Individual().setId("proband"))
                .setFamily(family)
                .setType(ClinicalAnalysis.Type.FAMILY);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);

        // We will create another clinical analysis with the same information. In this test, we will not lock clinical2
        clinicalAnalysis = new ClinicalAnalysis()
                .setId("clinical2")
                .setProband(new Individual().setId("proband"))
                .setFamily(family)
                .setType(ClinicalAnalysis.Type.FAMILY);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);

        // Update family not used in Clinical Analysis
        catalogManager.getFamilyManager().update(studyFqn, "family", new FamilyUpdateParams()
                        .setDescription(RandomStringUtils.randomAlphanumeric(10)),
                new QueryOptions(), ownerToken);

        Family familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), ownerToken).first();
        assertEquals(2, familyResult.getVersion());
        assertEquals(2, familyResult.getMembers().size());
        assertEquals(2, familyResult.getMembers().get(0).getVersion());
        assertEquals(2, familyResult.getMembers().get(1).getVersion());

        ClinicalAnalysis clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(),
                ownerToken).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(2, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), ownerToken).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(2, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());   // proband version

        // LOCK CLINICAL ANALYSIS
        catalogManager.getClinicalAnalysisManager().update(studyFqn, "clinical", new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), ownerToken);
        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), ownerToken).first();
        assertTrue(clinicalResult.isLocked());

        // Update family with version increment
        catalogManager.getFamilyManager().update(studyFqn, "family", new FamilyUpdateParams().setName("bl"), new QueryOptions(),
                ownerToken);

        familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), ownerToken).first();
        assertEquals(3, familyResult.getVersion());
        assertEquals(2, familyResult.getMembers().size());
        assertEquals(2, familyResult.getMembers().get(0).getVersion());
        assertEquals(2, familyResult.getMembers().get(1).getVersion());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), ownerToken).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(2, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), ownerToken).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().size());
    }

    @Test
    public void includeMemberIdOnly() throws CatalogException {
        createDummyFamily("family", true);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.MEMBERS.key() + "."
                + IndividualDBAdaptor.QueryParams.ID.key());
        DataResult<Family> family = familyManager.get(studyFqn, "family", options, ownerToken);

        for (Individual individual : family.first().getMembers()) {
            assertTrue(StringUtils.isNotEmpty(individual.getId()));
            assertTrue(StringUtils.isEmpty(individual.getName()));
            assertTrue(StringUtils.isEmpty(individual.getCreationDate()));
        }
    }

    /*
    *
    *  private DataResult<Family> createDummyFamily(String familyName) throws CatalogException {
        String fatherStr = RandomStringUtils.randomAlphanumeric(5);
        String motherStr = RandomStringUtils.randomAlphanumeric(5);
        String child1 = RandomStringUtils.randomAlphanumeric(5);
        String child2 = RandomStringUtils.randomAlphanumeric(5);
        String child3 = RandomStringUtils.randomAlphanumeric(5);

        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId(fatherStr).setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId(motherStr).setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId(fatherStr).setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setId(motherStr).setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setId(child1)
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList(child2, child3)))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId(child2)
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList(child1, child3)))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId(child3)
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList(child1, child2)))
                .setParentalConsanguinity(true);

        Family family = new Family(familyName, familyName, Arrays.asList(phenotype1, phenotype2), null,
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother), "", 5,
                Collections.emptyList(), Collections.emptyMap());

        return familyManager.create(studyFqn, family, QueryOptions.empty(), token);
    }
    * */

    private DataResult<Family> createDummyFamily(String familyName, boolean createMissingMembers) throws CatalogException {
        if (createMissingMembers) {
            Sample sample1 = new Sample().setId("sample1");
            catalogManager.getSampleManager().create(studyFqn, sample1, QueryOptions.empty(), ownerToken);

            Sample sample2 = new Sample().setId("sample2");
            catalogManager.getSampleManager().create(studyFqn, sample2, QueryOptions.empty(), ownerToken);

            Sample sample3 = new Sample().setId("sample3");
            catalogManager.getSampleManager().create(studyFqn, sample3, QueryOptions.empty(), ownerToken);
        }

        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father")
                .setSex(SexOntologyTermAnnotation.initMale())
                .setPhenotypes(Arrays.asList(phenotype1));
        Individual mother = new Individual().setId("mother")
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setPhenotypes(Arrays.asList(phenotype2));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father")
                .setSex(SexOntologyTermAnnotation.initMale())
                .setPhenotypes(Arrays.asList(phenotype1));
        Individual relMother = new Individual().setId("mother")
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setPhenotypes(Arrays.asList(phenotype2));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(phenotype1, phenotype2))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Arrays.asList(phenotype1))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId("child3")
                .setPhenotypes(Arrays.asList(phenotype1))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setParentalConsanguinity(true);

        List<Individual> members = null;
        List<String> memberIds = null;
        if (createMissingMembers) {
            members = Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother);
        } else {
            memberIds = Arrays.asList("father", "mother", "child1", "child2", "child3");
        }

        Family family = new Family(familyName, familyName, null, null, members, "", 5,
                Collections.emptyList(), Collections.emptyMap());

        OpenCGAResult<Family> familyOpenCGAResult = familyManager.create(studyFqn, family, memberIds, INCLUDE_RESULT, ownerToken);

        if (createMissingMembers) {
            catalogManager.getIndividualManager().update(studyFqn, relChild1.getId(),
                    new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId("sample1"))),
                    QueryOptions.empty(), ownerToken);
            catalogManager.getIndividualManager().update(studyFqn, relFather.getId(),
                    new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId("sample2"))),
                    QueryOptions.empty(), ownerToken);
            catalogManager.getIndividualManager().update(studyFqn, relMother.getId(),
                    new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId("sample3"))),
                    QueryOptions.empty(), ownerToken);
        }

        return familyOpenCGAResult;
    }

    @Test
    public void updateFamilyDisordersWhenIndividualDisorderIsUpdated() throws CatalogException {
        DataResult<Family> family = createDummyFamily("family", true);
        assertEquals(0, family.first().getDisorders().size());

        List<Disorder> disorderList = Arrays.asList(new Disorder().setId("disorder"));
        IndividualUpdateParams params = new IndividualUpdateParams().setDisorders(disorderList);

        catalogManager.getIndividualManager().update(studyFqn, "child1", params, new QueryOptions(), ownerToken);
        DataResult<Individual> child1 = catalogManager.getIndividualManager().get(studyFqn, "child1", QueryOptions.empty(), ownerToken);
        assertEquals(1, child1.first().getDisorders().size());
        assertEquals(3, child1.first().getVersion());

        family = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), ownerToken);
        assertEquals(1, family.first().getDisorders().size());

        disorderList = Collections.emptyList();
        params.setDisorders(disorderList);
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(IndividualDBAdaptor.QueryParams.DISORDERS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getIndividualManager().update(studyFqn, "child1", params, queryOptions, ownerToken);
        child1 = catalogManager.getIndividualManager().get(studyFqn, "child1", QueryOptions.empty(), ownerToken);
        assertEquals(0, child1.first().getDisorders().size());

        family = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), ownerToken);
        assertEquals(0, family.first().getDisorders().size());

        // Now we will update increasing the version. No changes should be produced in the family
        disorderList = Arrays.asList(new Disorder().setId("disorder"));
        params.setDisorders(disorderList);

        catalogManager.getIndividualManager().update(studyFqn, "child1", params, new QueryOptions(), ownerToken);
        child1 = catalogManager.getIndividualManager().get(studyFqn, "child1", QueryOptions.empty(), ownerToken);
        assertEquals(1, child1.first().getDisorders().size());
        assertEquals(5, child1.first().getVersion());

        family = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), ownerToken);
        for (Individual member : family.first().getMembers()) {
            if (member.getId().equals(child1.first().getId())) {
                assertEquals(child1.first().getVersion(), member.getVersion());
            }
        }
        assertEquals(1, family.first().getDisorders().size());
    }

    @Test
    public void disordersDistinctTest() throws CatalogException {
        DataResult<Family> family = createDummyFamily("family", true);

        List<Disorder> disorderList1 = Arrays.asList(new Disorder().setId("disorderId1").setName("disorderName1"));
        IndividualUpdateParams params1 = new IndividualUpdateParams().setDisorders(disorderList1);

        List<Disorder> disorderList2 = Arrays.asList(new Disorder().setId("disorderId2").setName("disorderName2"));
        IndividualUpdateParams params2 = new IndividualUpdateParams().setDisorders(disorderList2);


        catalogManager.getIndividualManager().update(studyFqn, "child1", params1, new QueryOptions(), ownerToken);
        catalogManager.getIndividualManager().update(studyFqn, "child2", params2, new QueryOptions(), ownerToken);

        OpenCGAResult<?> distinct = catalogManager.getFamilyManager().distinct(studyFqn, "disorders.name", new Query(), ownerToken);

        System.out.println(distinct);
        assertEquals(2, distinct.getNumResults());
    }


    @Test
    public void createFamilyDuo() throws CatalogException {
        Family family = new Family()
                .setId("test")
                .setPhenotypes(Arrays.asList(new Phenotype("E00", "blabla", "blabla")))
                .setMembers(Arrays.asList(new Individual().setId("proband").setSex(SexOntologyTermAnnotation.initMale()),
                        new Individual().setFather(new Individual().setId("proband")).setId("child")
                                .setSex(SexOntologyTermAnnotation.initFemale())));
        DataResult<Family> familyDataResult = familyManager.create(studyFqn, family, INCLUDE_RESULT, ownerToken);

        assertEquals(2, familyDataResult.first().getMembers().size());
    }

    @Test
    public void createFamilyMissingMember() throws CatalogException {
        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father")
                .setSex(SexOntologyTermAnnotation.initMale())
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId("mother")
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father")
                .setSex(SexOntologyTermAnnotation.initMale())
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Collections.singletonList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", "Martinez-Martinez", Arrays.asList(phenotype1, phenotype2), null,
                Arrays.asList(relFather, relChild1, relChild2), "", 3, Collections.emptyList(), Collections.emptyMap());

        family = familyManager.create(studyFqn, family, new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), ownerToken).first();
        assertEquals(3, family.getMembers().size());
        assertTrue(Arrays.asList(relFather.getId(), relChild2.getId(), relChild1.getId())
                .containsAll(family.getMembers().stream().map(Individual::getId).collect(Collectors.toList())));
    }

    @Test
    public void createFamilyPhenotypeNotPassed() throws CatalogException {
        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis3", "dis3", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", "Martinez-Martinez", Arrays.asList(phenotype1, phenotype2), null,
                Arrays.asList(relFather, relMother, relChild1, relChild2), "", 4, Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not present in any member of the family");
        familyManager.create(studyFqn, family, QueryOptions.empty(), ownerToken);
    }

    @Test(expected = CatalogException.class)
    public void createFamilyRepeatedMember() throws CatalogException {
        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father")
                .setSex(SexOntologyTermAnnotation.initMale())
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId("mother")
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father")
                .setSex(SexOntologyTermAnnotation.initMale())
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setId("mother")
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", "Martinez-Martinez", Arrays.asList(phenotype1, phenotype2), null,
                Arrays.asList(relFather, relMother, relChild1, relChild2, relChild1), "", -1, Collections.emptyList(), Collections.emptyMap
                ());

        DataResult<Family> familyDataResult = familyManager.create(studyFqn, family, INCLUDE_RESULT, ownerToken);
        assertEquals(4, familyDataResult.first().getMembers().size());
    }

    @Test
    public void createEmptyFamily() throws CatalogException {
        Family family = new Family("xxx", "xxx", null, null, null, "", -1, Collections.emptyList(), Collections.emptyMap());
        DataResult<Family> familyDataResult = familyManager.create(studyFqn, family, INCLUDE_RESULT, ownerToken);
        assertEquals(1, familyDataResult.getNumResults());
    }

//    @Test
//    public void updateFamilyMembers() throws CatalogException, JsonProcessingException {
//        DataResult<Family> originalFamily = createDummyFamily();
//
//        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
//        Individual mother = new Individual().setName("mother2").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));
//
//        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
//        // ingesting references to exactly the same object and this test would not work exactly the same way.
//        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
//        Individual relMother = new Individual().setName("mother2").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));
//
//        Individual relChild1 = new Individual().setName("child3")
//                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
//                .setFather(father)
//                .setMother(mother)
//                .setParentalConsanguinity(true);
//
//        Family family = new Family();
//        family.setMembers(Arrays.asList(relChild1, relFather, relMother));
//        ObjectMapper jsonObjectMapper = catalogManagerResource.generateNewObjectMapper();
//
//        ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(family));
//        params = new ObjectMap(FamilyDBAdaptor.QueryParams.MEMBERS.key(), params.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()));
//
//        DataResult<Family> updatedFamily = familyManager.update(studyFqn, originalFamily.first().getName(), params, QueryOptions.empty(),
//                token);
//
//        assertEquals(3, updatedFamily.first().getMembers().size());
//        // Other parameters from the family should not have been stored in the database
//        assertEquals(null, updatedFamily.first().getMembers().get(0).getName());
//
//        // We store the ids when the family was first created
//        Set<Long> originalFamilyIds = originalFamily.first().getMembers().stream()
//                .map(m -> m.getId())
//                .collect(Collectors.toSet());
//
//        // Only one id should be the same as in originalFamilyIds (father id)
//        for (Individual relatives : updatedFamily.first().getMembers()) {
//            if (relatives.getFather().getId() > 0) {
//                assertTrue(originalFamilyIds.contains(relatives.getFather().getId()));
//            }
//            if (relatives.getMother().getId() > 0) {
//                assertTrue(!originalFamilyIds.contains(relatives.getMother().getId()));
//            }
//        }
//    }

    @Test
    public void updateFamilyMissingMember() throws CatalogException {
        DataResult<Family> originalFamily = createDummyFamily("Martinez-Martinez", true);

        FamilyUpdateParams updateParams = new FamilyUpdateParams().setMembers(Arrays.asList(
                new IndividualReferenceParam().setId("child3"),
                new IndividualReferenceParam().setId("father")));

        Family family = familyManager.update(studyFqn, originalFamily.first().getId(), updateParams,
                new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), ownerToken).first();
        assertEquals(2, family.getMembers().size());
        assertEquals(2, family.getRoles().size());
        assertTrue(Arrays.asList("child3", "father")
                .containsAll(family.getMembers().stream().map(Individual::getId).collect(Collectors.toList())));
    }

    @Test
    public void recalculateRoles() throws CatalogException {
        DataResult<Family> originalFamily = createDummyFamily("Martinez-Martinez", true);

        FamilyUpdateParams updateParams = null;
        QueryOptions options = new QueryOptions(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true);

        assertEquals(1, familyManager.update(studyFqn, originalFamily.first().getId(), updateParams, options, ownerToken).getNumUpdated());
    }

    @Test
    public void updateFamilyQualityControl() throws CatalogException {
        DataResult<Family> originalFamily = createDummyFamily("Martinez-Martinez", true);

        FamilyQualityControl qualityControl = new FamilyQualityControl(null, Arrays.asList("file1", "file2"),
                Collections.singletonList(new ClinicalComment("author", "message", Collections.singletonList("tag"), "date")));

        FamilyUpdateParams updateParams = new FamilyUpdateParams().setQualityControl(qualityControl);

        DataResult<Family> updatedFamily = familyManager.update(studyFqn, originalFamily.first().getId(),
                updateParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, updatedFamily.getNumUpdated());

        updatedFamily = familyManager.get(studyFqn, originalFamily.first().getId(), QueryOptions.empty(), ownerToken);
        assertTrue(Arrays.asList("file1", "file2").containsAll(updatedFamily.first().getQualityControl().getFiles()));
        assertEquals(1, updatedFamily.first().getQualityControl().getComments().size());
        assertEquals("author", updatedFamily.first().getQualityControl().getComments().get(0).getAuthor());
        assertEquals("message", updatedFamily.first().getQualityControl().getComments().get(0).getMessage());
        assertEquals("tag", updatedFamily.first().getQualityControl().getComments().get(0).getTags().get(0));
        assertEquals("date", updatedFamily.first().getQualityControl().getComments().get(0).getDate());
    }

    // Test versioning
    @Test
    public void incrementVersionTest() throws CatalogException {
        Family dummyFamily1 = DummyModelUtils.getDummyFamily();
        Family dummyFamily2 = DummyModelUtils.getDummyFamily();

        for (int i = dummyFamily1.getMembers().size() - 1; i >= 0; i--) {
            catalogManager.getIndividualManager().create(studyFqn, dummyFamily1.getMembers().get(i), QueryOptions.empty(), ownerToken);
        }

        List<String> members = dummyFamily1.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        dummyFamily1.setMembers(null);
        catalogManager.getFamilyManager().create(studyFqn, dummyFamily1, members, QueryOptions.empty(), ownerToken);
        dummyFamily2.setMembers(null);
        catalogManager.getFamilyManager().create(studyFqn, dummyFamily2, members, QueryOptions.empty(), ownerToken);

        OpenCGAResult<Family> result = catalogManager.getFamilyManager().get(studyFqn, Arrays.asList(dummyFamily1.getId(),
                dummyFamily2.getId()), QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        for (Family family : result.getResults()) {
            if (family.getId().equals(dummyFamily1.getId())) {
                assertEquals(2, family.getVersion());
            } else if (family.getId().equals(dummyFamily2.getId())) {
                assertEquals(1, family.getVersion());
            } else {
                fail();
            }
        }

        catalogManager.getFamilyManager().update(studyFqn, dummyFamily1.getId(), new FamilyUpdateParams().setName("name"),
                QueryOptions.empty(), ownerToken);
        catalogManager.getFamilyManager().update(studyFqn, dummyFamily1.getId(), new FamilyUpdateParams().setName("name22"),
                QueryOptions.empty(), ownerToken);
        result = catalogManager.getFamilyManager().get(studyFqn, Arrays.asList(dummyFamily1.getId(), dummyFamily2.getId()),
                QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        assertEquals(4, result.first().getVersion());
        assertEquals(1, result.getResults().get(1).getVersion());

        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.ID.key(), dummyFamily1.getId())
                .append(Constants.ALL_VERSIONS, true);
        result = catalogManager.getFamilyManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);

        assertEquals(1, result.getResults().get(0).getVersion());
        assertEquals(2, result.getResults().get(1).getVersion());
        assertEquals(3, result.getResults().get(2).getVersion());
        assertEquals("name", result.getResults().get(2).getName());
        assertEquals(4, result.getResults().get(3).getVersion());
        assertEquals("name22", result.getResults().get(3).getName());
    }

    // Test updates and relationships
    @Test
    public void memberReferenceTest() throws CatalogException {
        Family dummyFamily1 = DummyModelUtils.getDummyFamily();
        Family dummyFamily2 = DummyModelUtils.getDummyFamily();

        for (int i = dummyFamily1.getMembers().size() - 1; i >= 0; i--) {
            catalogManager.getIndividualManager().create(studyFqn, dummyFamily1.getMembers().get(i), QueryOptions.empty(), ownerToken);
        }

        List<String> members = dummyFamily1.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        dummyFamily1.setMembers(null);
        catalogManager.getFamilyManager().create(studyFqn, dummyFamily1, members, QueryOptions.empty(), ownerToken);
        dummyFamily2.setMembers(null);
        catalogManager.getFamilyManager().create(studyFqn, dummyFamily2, members, QueryOptions.empty(), ownerToken);

        OpenCGAResult<Individual> result = catalogManager.getIndividualManager().get(studyFqn, members, QueryOptions.empty(), ownerToken);
        for (Individual member : result.getResults()) {
            assertEquals(2, member.getFamilyIds().size());
            assertTrue(member.getFamilyIds().containsAll(Arrays.asList(dummyFamily1.getId(), dummyFamily2.getId())));
        }

        // Update family id
        catalogManager.getFamilyManager().update(studyFqn, dummyFamily1.getId(), new FamilyUpdateParams().setId("newId"), QueryOptions.empty(),
                ownerToken);
        result = catalogManager.getIndividualManager().get(studyFqn, members, QueryOptions.empty(), ownerToken);
        for (Individual member : result.getResults()) {
            assertEquals(2, member.getFamilyIds().size());
            assertTrue(member.getFamilyIds().containsAll(Arrays.asList("newId", dummyFamily2.getId())));
        }

        // Delete family1
        catalogManager.getFamilyManager().delete(studyFqn, Collections.singletonList("newId"), QueryOptions.empty(), ownerToken);
        result = catalogManager.getIndividualManager().get(studyFqn, members, QueryOptions.empty(), ownerToken);
        for (Individual member : result.getResults()) {
            assertEquals(1, member.getFamilyIds().size());
            assertEquals(dummyFamily2.getId(), member.getFamilyIds().get(0));
        }
    }

    // Test update when use in CA
    @Test
    public void updateInUseInCATest() throws CatalogException {
        Family family = DummyModelUtils.getDummyCaseFamily("family1");

        for (int i = family.getMembers().size() - 1; i >= 0; i--) {
            catalogManager.getIndividualManager().create(studyFqn, family.getMembers().get(i), QueryOptions.empty(), ownerToken);
        }

        List<String> members = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        family.setMembers(null);
        catalogManager.getFamilyManager().create(studyFqn, family, members, QueryOptions.empty(), ownerToken);

        // Unlocked cases
        ClinicalAnalysis case1 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);
        ClinicalAnalysis case2 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);

        // locked true
        ClinicalAnalysis case3 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);

        catalogManager.getClinicalAnalysisManager().create(studyFqn, case1, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case2, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case3, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case3.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), ownerToken);

        // Update family id
        catalogManager.getFamilyManager().update(studyFqn, family.getId(), new FamilyUpdateParams().setId("newId"), QueryOptions.empty(),
                ownerToken);

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().get(studyFqn,
                Arrays.asList(case1.getId(), case2.getId(), case3.getId()), QueryOptions.empty(), ownerToken);
        case1 = result.getResults().get(0);
        case2 = result.getResults().get(1);
        case3 = result.getResults().get(2);

        assertEquals(2, case1.getFamily().getVersion());
        assertEquals(2, case2.getFamily().getVersion());
        assertEquals(1, case3.getFamily().getVersion());
    }

    // Test when in use in CA
    @Test
    public void updateDeleteInUseInCATest() throws CatalogException {
        Family family = DummyModelUtils.getDummyCaseFamily("family1");

        for (int i = family.getMembers().size() - 1; i >= 0; i--) {
            catalogManager.getIndividualManager().create(studyFqn, family.getMembers().get(i), QueryOptions.empty(), ownerToken);
        }

        List<String> members = family.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
        family.setMembers(null);
        catalogManager.getFamilyManager().create(studyFqn, family, members, QueryOptions.empty(), ownerToken);

        // Unlocked cases
        ClinicalAnalysis case1 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);
        ClinicalAnalysis case2 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);

        // locked true
        ClinicalAnalysis case3 = DummyModelUtils.getDummyClinicalAnalysis(family.getMembers().get(0), family, null);

        catalogManager.getClinicalAnalysisManager().create(studyFqn, case1, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case2, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case3, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case3.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), ownerToken);

        // Delete family
        try {
            catalogManager.getFamilyManager().delete(studyFqn, Collections.singletonList(family.getId()), QueryOptions.empty(), ownerToken);
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("in use in Clinical Analyses"));
        }

        // unlock case3
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case3.getId(),
                new ClinicalAnalysisUpdateParams().setLocked(false), QueryOptions.empty(), ownerToken);

        try {
            catalogManager.getFamilyManager().delete(studyFqn, Collections.singletonList(family.getId()), QueryOptions.empty(), ownerToken);
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("in use in Clinical Analyses"));
        }
    }

    @Test
    public void updateFamilyMembers() throws CatalogException {
        Individual child = DummyModelUtils.getDummyIndividual("child", SexOntologyTermAnnotation.initMale(), null, null, null);
        Individual father = DummyModelUtils.getDummyIndividual("father", SexOntologyTermAnnotation.initMale(), null, null, null);
        Individual mother = DummyModelUtils.getDummyIndividual("mother", SexOntologyTermAnnotation.initFemale(), null, null, null);
        child.setFather(father);
        child.setMother(mother);

        catalogManager.getIndividualManager().create(studyFqn, father, QueryOptions.empty(), ownerToken);
        catalogManager.getIndividualManager().create(studyFqn, mother, QueryOptions.empty(), ownerToken);
        catalogManager.getIndividualManager().create(studyFqn, child, QueryOptions.empty(), ownerToken);

        Family family = DummyModelUtils.getDummyFamily("family");
        family.setMembers(null);
        catalogManager.getFamilyManager().create(studyFqn, family, Collections.singletonList(child.getId()), QueryOptions.empty(),
                ownerToken);

        FamilyUpdateParams updateParams = new FamilyUpdateParams().setMembers(Arrays.asList(
                new IndividualReferenceParam(child.getId(), child.getUuid()),
                new IndividualReferenceParam(father.getId(), child.getUuid())
        ));
        catalogManager.getFamilyManager().update(studyFqn, family.getId(), updateParams, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void testFacet() throws CatalogException {
        createDummyFamily("Other-Family-Name-1", true);

        OpenCGAResult<Family> results = catalogManager.getFamilyManager().search(studyFqn, new Query(), QueryOptions.empty(), ownerToken);
        System.out.println("results.getResults() = " + results.getResults());
        OpenCGAResult<FacetField> facets = catalogManager.getFamilyManager().facet(studyFqn, new Query(), "max(version)", normalToken1);

        long maxValue = 0;
        for (Family result : results.getResults()) {
            if (result.getVersion() > maxValue) {
                maxValue = result.getVersion();
            }
        }

        Assert.assertEquals(1, facets.getResults().size());
        for (FacetField result : facets.getResults()) {
            Assert.assertEquals(max.name(), result.getAggregationName());
            Assert.assertEquals(1, result.getAggregationValues().size());
            Assert.assertEquals(maxValue, result.getAggregationValues().get(0), 0.0001);
        }
    }
}
