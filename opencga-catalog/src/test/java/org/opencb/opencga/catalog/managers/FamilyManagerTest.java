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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.update.FamilyUpdateParams;
import org.opencb.opencga.catalog.models.update.IndividualUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.AclParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 11/05/17.
 */
public class FamilyManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    public final static String STUDY = "user@1000G:phase1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    private FamilyManager familyManager;
    protected String sessionIdUser;

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", Study.Type.TRIO, null, "Done", null, null, null, null,
                null, null, null, null, sessionIdUser);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void createFamily() throws CatalogException {
        QueryResult<Family> familyQueryResult = createDummyFamily("Martinez-Martinez", true);

        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(5, familyQueryResult.first().getMembers().size());
        assertEquals(2, familyQueryResult.first().getPhenotypes().size());

        boolean motherIdUpdated = false;
        boolean fatherIdUpdated = false;
        for (Individual relatives : familyQueryResult.first().getMembers()) {
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
        familyQueryResult = createDummyFamily("Other-Family-Name", false);

        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(5, familyQueryResult.first().getMembers().size());
        assertEquals(2, familyQueryResult.first().getPhenotypes().size());

        motherIdUpdated = false;
        fatherIdUpdated = false;
        for (Individual relatives : familyQueryResult.first().getMembers()) {
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
    public void getFamilyWithOnlyAllowedMembers() throws CatalogException, IOException {
        createDummyFamily("Martinez-Martinez", true);

        catalogManager.getUserManager().create("user2", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.GUEST, null, null);
        String token = catalogManager.getUserManager().login("user2", PASSWORD);

        try {
            familyManager.get(STUDY, "Martinez-Martinez", QueryOptions.empty(), token);
            fail("Expected authorization exception. user2 should not be able to see the study");
        } catch (CatalogAuthorizationException ignored) {
        }

        familyManager.updateAcl(STUDY, Collections.singletonList("Martinez-Martinez"), "user2", new AclParams("VIEW", AclParams.Action.SET),
                sessionIdUser);
        QueryResult<Family> familyQueryResult = familyManager.get(STUDY, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(0, familyQueryResult.first().getMembers().size());

        catalogManager.getIndividualManager().updateAcl(STUDY, Collections.singletonList("child2"), "user2",
                new Individual.IndividualAclParams("VIEW", AclParams.Action.SET, "", false), sessionIdUser);
        familyQueryResult = familyManager.get(STUDY, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(1, familyQueryResult.first().getMembers().size());
        assertEquals("child2", familyQueryResult.first().getMembers().get(0).getId());

        catalogManager.getIndividualManager().updateAcl(STUDY, Collections.singletonList("child3"), "user2",
                new Individual.IndividualAclParams("VIEW", AclParams.Action.SET, "", false), sessionIdUser);
        familyQueryResult = familyManager.get(STUDY, "Martinez-Martinez", QueryOptions.empty(), token);
        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(2, familyQueryResult.first().getMembers().size());
        assertEquals("child2", familyQueryResult.first().getMembers().get(0).getId());
        assertEquals("child3", familyQueryResult.first().getMembers().get(1).getId());

        familyQueryResult = familyManager.get(STUDY, "Martinez-Martinez",
                new QueryOptions(QueryOptions.EXCLUDE, FamilyDBAdaptor.QueryParams.MEMBERS.key()), token);
        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(null, familyQueryResult.first().getMembers());

        familyQueryResult = familyManager.get(STUDY, "Martinez-Martinez",
                new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.MEMBERS.key() + "."
                        + IndividualDBAdaptor.QueryParams.NAME.key()),
                token);
        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(null, familyQueryResult.first().getName());
        assertEquals(2, familyQueryResult.first().getMembers().size());
        assertEquals(null, familyQueryResult.first().getMembers().get(0).getId());
        assertEquals(null, familyQueryResult.first().getMembers().get(1).getId());
        assertEquals("child2", familyQueryResult.first().getMembers().get(0).getName());
        assertEquals("child3", familyQueryResult.first().getMembers().get(1).getName());
    }


    @Test
    public void includeMemberIdOnly() throws CatalogException {
        createDummyFamily("family", true);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.MEMBERS.key() + "."
                + IndividualDBAdaptor.QueryParams.ID.key());
        QueryResult<Family> family = familyManager.get(STUDY, "family", options, sessionIdUser);

        for (Individual individual : family.first().getMembers()) {
            assertTrue(StringUtils.isNotEmpty(individual.getId()));
            assertTrue(StringUtils.isEmpty(individual.getName()));
            assertTrue(StringUtils.isEmpty(individual.getCreationDate()));
        }
    }

    /*
    *
    *  private QueryResult<Family> createDummyFamily(String familyName) throws CatalogException {
        String fatherStr = org.opencb.commons.utils.StringUtils.randomString(5);
        String motherStr = org.opencb.commons.utils.StringUtils.randomString(5);
        String child1 = org.opencb.commons.utils.StringUtils.randomString(5);
        String child2 = org.opencb.commons.utils.StringUtils.randomString(5);
        String child3 = org.opencb.commons.utils.StringUtils.randomString(5);

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

        return familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }
    * */

    private QueryResult<Family> createDummyFamily(String familyName, boolean createMissingMembers) throws CatalogException {
        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father").setPhenotypes(Arrays.asList(phenotype1));
        Individual mother = new Individual().setId("mother").setPhenotypes(Arrays.asList(phenotype2));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setPhenotypes(Arrays.asList(phenotype1));
        Individual relMother = new Individual().setId("mother").setPhenotypes(Arrays.asList(phenotype2));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(phenotype1, phenotype2))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child2", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Arrays.asList(phenotype1))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId("child3")
                .setPhenotypes(Arrays.asList(phenotype1))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child2")))
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

        return familyManager.create(STUDY, family, memberIds, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updateFamilyDisordersWhenIndividualDisorderIsUpdated() throws CatalogException {
        QueryResult<Family> family = createDummyFamily("family", true);
        assertEquals(0, family.first().getDisorders().size());

        List<Disorder> disorderList = Arrays.asList(new Disorder().setId("disorder"));
        IndividualUpdateParams params = new IndividualUpdateParams().setDisorders(disorderList);

        catalogManager.getIndividualManager().update(STUDY, "child1", params, new QueryOptions(), sessionIdUser);
        QueryResult<Individual> child1 = catalogManager.getIndividualManager().get(STUDY, "child1", QueryOptions.empty(), sessionIdUser);
        assertEquals(1, child1.first().getDisorders().size());

        family = catalogManager.getFamilyManager().get(STUDY, "family", QueryOptions.empty(), sessionIdUser);
        assertEquals(1, family.first().getDisorders().size());

        disorderList = Collections.emptyList();
        params.setDisorders(disorderList);
        catalogManager.getIndividualManager().update(STUDY, "child1", params, new QueryOptions(), sessionIdUser);
        child1 = catalogManager.getIndividualManager().get(STUDY, "child1", QueryOptions.empty(), sessionIdUser);
        assertEquals(0, child1.first().getDisorders().size());

        family = catalogManager.getFamilyManager().get(STUDY, "family", QueryOptions.empty(), sessionIdUser);
        assertEquals(0, family.first().getDisorders().size());

        // Now we will update increasing the version. No changes should be produced in the family
        disorderList = Arrays.asList(new Disorder().setId("disorder"));
        params.setDisorders(disorderList);

        catalogManager.getIndividualManager().update(STUDY, "child1", params, new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);
        child1 = catalogManager.getIndividualManager().get(STUDY, "child1", QueryOptions.empty(), sessionIdUser);
        assertEquals(1, child1.first().getDisorders().size());
        assertEquals(2, child1.first().getVersion());

        family = catalogManager.getFamilyManager().get(STUDY, "family", QueryOptions.empty(), sessionIdUser);
        assertEquals(0, family.first().getDisorders().size());
    }

    @Test
    public void createFamilyMissingMultiple() throws CatalogException {
        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child2", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId("child3")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1")))
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", "Martinez-Martinez", Arrays.asList(phenotype1, phenotype2), null,
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother), "", 5,
                Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Incomplete sibling information");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyMissingMultiple2() throws CatalogException {
        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child2", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId("child3")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child20")))
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", "Martinez-Martinez", Arrays.asList(phenotype1, phenotype2), null,
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother), "", -1,
                Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Incomplete sibling information");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyDuo() throws CatalogException {
        Family family = new Family()
                .setId("test")
                .setPhenotypes(Arrays.asList(new Phenotype("E00", "blabla", "blabla")))
                .setMembers(Arrays.asList(new Individual().setId("proband").setSex(IndividualProperty.Sex.MALE),
                        new Individual().setFather(new Individual().setId("proband")).setId("child")
                                .setSex(IndividualProperty.Sex.FEMALE)));
        QueryResult<Family> familyQueryResult = familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);

        assertEquals(2, familyQueryResult.first().getMembers().size());
    }

    @Test
    public void createFamilyMissingMember() throws CatalogException {
        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Collections.singletonList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", "Martinez-Martinez", Arrays.asList(phenotype1, phenotype2), null,
                Arrays.asList(relFather, relChild1, relChild2), "", 3, Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not present in the members list");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
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
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyRepeatedMember() throws CatalogException {
        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", "Martinez-Martinez", Arrays.asList(phenotype1, phenotype2), null,
                Arrays.asList(relFather, relMother, relChild1, relChild2, relChild1), "", -1, Collections.emptyList(), Collections.emptyMap
                ());

        QueryResult<Family> familyQueryResult = familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
        assertEquals(4, familyQueryResult.first().getMembers().size());
    }

    @Test
    public void createEmptyFamily() throws CatalogException {
        Family family = new Family("xxx", "xxx", null, null, null, "", -1, Collections.emptyList(), Collections.emptyMap());
        QueryResult<Family> familyQueryResult = familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, familyQueryResult.getNumResults());
    }

//    @Test
//    public void updateFamilyMembers() throws CatalogException, JsonProcessingException {
//        QueryResult<Family> originalFamily = createDummyFamily();
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
//        QueryResult<Family> updatedFamily = familyManager.update(STUDY, originalFamily.first().getName(), params, QueryOptions.empty(),
//                sessionIdUser);
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
    public void updateFamilyMissingMember() throws CatalogException, JsonProcessingException {
        QueryResult<Family> originalFamily = createDummyFamily("Martinez-Martinez", true);

        Individual father = new Individual().setId("father");
        Individual mother = new Individual().setId("mother2");

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));

        Individual relChild1 = new Individual().setId("child3")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);

        FamilyUpdateParams updateParams = new FamilyUpdateParams().setMembers(Arrays.asList("child3", "father"));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not present in the members list");
        familyManager.update(STUDY, originalFamily.first().getId(), updateParams, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updateFamilyPhenotype() throws JsonProcessingException, CatalogException {
        QueryResult<Family> originalFamily = createDummyFamily("Martinez-Martinez", true);

        Phenotype phenotype1 = new Phenotype("dis1", "New name", "New source");
        Phenotype phenotype2 = new Phenotype("dis2", "New name", "New source");
        Phenotype phenotype3 = new Phenotype("dis3", "New name", "New source");

        FamilyUpdateParams updateParams = new FamilyUpdateParams().setPhenotypes(Arrays.asList(phenotype1, phenotype2, phenotype3));

        QueryResult<Family> updatedFamily = familyManager.update(STUDY, originalFamily.first().getName(), updateParams, QueryOptions.empty(),
                sessionIdUser);
        assertEquals(3, updatedFamily.first().getPhenotypes().size());

        // Only one id should be the same as in originalFamilyIds (father id)
        for (Phenotype phenotype : updatedFamily.first().getPhenotypes()) {
            assertEquals("New name", phenotype.getName());
            assertEquals("New source", phenotype.getSource());
        }
    }

    @Test
    public void updateFamilyMissingPhenotype() throws JsonProcessingException, CatalogException {
        QueryResult<Family> originalFamily = createDummyFamily("Martinez-Martinez", true);

        Phenotype phenotype1 = new Phenotype("dis1", "New name", "New source");

        FamilyUpdateParams updateParams = new FamilyUpdateParams().setPhenotypes(Collections.singletonList(phenotype1));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not present in any member of the family");
        familyManager.update(STUDY, originalFamily.first().getId(), updateParams, QueryOptions.empty(), sessionIdUser);
    }

}
