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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD).getToken();

        long projectId = catalogManager.getProjectManager().create("Project about some genomes", "1000G", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(String.valueOf(projectId), "Phase 1", "phase1", Study.Type.TRIO, null, "Done", null, null, null, null, null, null, null, null, sessionIdUser);

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void createFamily() throws CatalogException {
        QueryResult<Family> familyQueryResult = createDummyFamily("Martinez-Martinez");

        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(5, familyQueryResult.first().getMembers().size());
        assertEquals(2, familyQueryResult.first().getPhenotypes().size());

        boolean motherIdUpdated = false;
        boolean fatherIdUpdated = false;
        for (Individual relatives : familyQueryResult.first().getMembers()) {
            if (relatives.getMother().getId() > 0) {
                motherIdUpdated = true;
            }
            if (relatives.getFather().getId() > 0) {
                fatherIdUpdated = true;
            }
        }

        assertTrue("Mother id not associated to any children", motherIdUpdated);
        assertTrue("Father id not associated to any children", fatherIdUpdated);

        // Create family again with individuals already created
        familyQueryResult = createDummyFamily("Other-Family-Name");

        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(5, familyQueryResult.first().getMembers().size());
        assertEquals(2, familyQueryResult.first().getPhenotypes().size());

        motherIdUpdated = false;
        fatherIdUpdated = false;
        for (Individual relatives : familyQueryResult.first().getMembers()) {
            if (relatives.getMother().getId() > 0) {
                motherIdUpdated = true;
            }
            if (relatives.getFather().getId() > 0) {
                fatherIdUpdated = true;
            }
        }

        assertTrue("Mother id not associated to any children", motherIdUpdated);
        assertTrue("Father id not associated to any children", fatherIdUpdated);
    }

    private QueryResult<Family> createDummyFamily(String familyName) throws CatalogException {
        OntologyTerm phenotype1 = new OntologyTerm("dis1", "Phenotype 1", "HPO");
        OntologyTerm phenotype2 = new OntologyTerm("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual mother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setName("child1")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child2", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setName("child2")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setName("child3")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child2")))
                .setParentalConsanguinity(true);

        Family family = new Family(familyName, Arrays.asList(phenotype1, phenotype2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother),"", Collections.emptyList(), Collections.emptyMap());

        return familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyMissingMultiple() throws CatalogException {
        OntologyTerm phenotype1 = new OntologyTerm("dis1", "Phenotype 1", "HPO");
        OntologyTerm phenotype2 = new OntologyTerm("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual mother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setName("child1")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child2", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setName("child2")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setName("child3")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1")))
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(phenotype1, phenotype2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Incomplete sibling information");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyMissingMultiple2() throws CatalogException {
        OntologyTerm phenotype1 = new OntologyTerm("dis1", "Phenotype 1", "HPO");
        OntologyTerm phenotype2 = new OntologyTerm("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual mother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setName("child1")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child2", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setName("child2")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setName("child3")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child20")))
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(phenotype1, phenotype2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Incomplete sibling information");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyDuo() throws CatalogException {
        Family family = new Family()
                .setName("test")
                .setPhenotypes(Arrays.asList(new OntologyTerm("E00", "blabla", "blabla")))
                .setMembers(Arrays.asList(new Individual().setName("proband").setSex(Individual.Sex.MALE),
                        new Individual().setFather(new Individual().setName("proband")).setName("child").setSex(Individual.Sex.FEMALE)));
        QueryResult<Family> familyQueryResult = familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);

        assertEquals(2, familyQueryResult.first().getMembers().size());
    }

    @Test
    public void createFamilyMissingMember() throws CatalogException {
        OntologyTerm phenotype1 = new OntologyTerm("dis1", "Phenotype 1", "HPO");
        OntologyTerm phenotype2 = new OntologyTerm("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual mother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));

        Individual relChild1 = new Individual().setName("child1")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setName("child2")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(phenotype1, phenotype2),
                Arrays.asList(relFather, relChild1, relChild2),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not present in the members list");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyPhenotypeNotPassed() throws CatalogException {
        OntologyTerm phenotype1 = new OntologyTerm("dis1", "Phenotype 1", "HPO");
        OntologyTerm phenotype2 = new OntologyTerm("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual mother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setName("child1")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis3", "dis3", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setName("child2")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(phenotype1, phenotype2),
                Arrays.asList(relFather, relMother, relChild1, relChild2),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not present in any member of the family");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyRepeatedMember() throws CatalogException {
        OntologyTerm phenotype1 = new OntologyTerm("dis1", "Phenotype 1", "HPO");
        OntologyTerm phenotype2 = new OntologyTerm("dis2", "Phenotype 2", "HPO");

        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual mother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setName("child1")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setName("child2")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(phenotype1, phenotype2),
                Arrays.asList(relFather, relMother, relChild1, relChild2, relChild1),"", Collections.emptyList(), Collections.emptyMap());

        QueryResult<Family> familyQueryResult = familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
        assertEquals(4, familyQueryResult.first().getMembers().size());
    }

    @Test
    public void createEmptyFamily() throws CatalogException {
        Family family = new Family("xxx", null, null,"", Collections.emptyList(), Collections.emptyMap());
        QueryResult<Family> familyQueryResult = familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, familyQueryResult.getNumResults());
    }

//    @Test
//    public void updateFamilyMembers() throws CatalogException, JsonProcessingException {
//        QueryResult<Family> originalFamily = createDummyFamily();
//
//        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
//        Individual mother = new Individual().setName("mother2").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));
//
//        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
//        // ingesting references to exactly the same object and this test would not work exactly the same way.
//        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
//        Individual relMother = new Individual().setName("mother2").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));
//
//        Individual relChild1 = new Individual().setName("child3")
//                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis2", "dis2", "OT")))
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
        QueryResult<Family> originalFamily = createDummyFamily("Martinez-Martinez");

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother2");

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));

        Individual relChild1 = new Individual().setName("child3")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);


        Family family = new Family();
        family.setMembers(Arrays.asList(relChild1, relFather));
        ObjectMapper jsonObjectMapper = catalogManagerResource.generateNewObjectMapper();

        ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(family));
        params = new ObjectMap(FamilyDBAdaptor.QueryParams.MEMBERS.key(), params.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not present in the members list");
        familyManager.update(STUDY, originalFamily.first().getName(), params, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updateFamilyPhenotype() throws JsonProcessingException, CatalogException {
        QueryResult<Family> originalFamily = createDummyFamily("Martinez-Martinez");

        OntologyTerm phenotype1 = new OntologyTerm("dis1", "New name", "New source");
        OntologyTerm phenotype2 = new OntologyTerm("dis2", "New name", "New source");
        OntologyTerm phenotype3 = new OntologyTerm("dis3", "New name", "New source");

        Family family = new Family();
        family.setPhenotypes(Arrays.asList(phenotype1, phenotype2, phenotype3));
        ObjectMapper jsonObjectMapper = catalogManagerResource.generateNewObjectMapper();

        ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(family));
        params = new ObjectMap(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), params.get(FamilyDBAdaptor.QueryParams.PHENOTYPES.key()));

        QueryResult<Family> updatedFamily = familyManager.update(STUDY, originalFamily.first().getName(), params, QueryOptions.empty(),
                sessionIdUser);

        assertEquals(3, updatedFamily.first().getPhenotypes().size());

        // Only one id should be the same as in originalFamilyIds (father id)
        for (OntologyTerm phenotype : updatedFamily.first().getPhenotypes()) {
            assertEquals("New name", phenotype.getName());
            assertEquals("New source", phenotype.getSource());
        }
    }

    @Test
    public void updateFamilyMissingPhenotype() throws JsonProcessingException, CatalogException {
        QueryResult<Family> originalFamily = createDummyFamily("Martinez-Martinez");

        OntologyTerm phenotype1 = new OntologyTerm("dis1", "New name", "New source");

        Family family = new Family();
        family.setPhenotypes(Arrays.asList(phenotype1));
        ObjectMapper jsonObjectMapper = catalogManagerResource.generateNewObjectMapper();

        ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(family));
        params = new ObjectMap(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), params.get(FamilyDBAdaptor.QueryParams.PHENOTYPES.key()));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not present in any member of the family");
        familyManager.update(STUDY, originalFamily.first().getName(), params, QueryOptions.empty(), sessionIdUser);
    }

}
