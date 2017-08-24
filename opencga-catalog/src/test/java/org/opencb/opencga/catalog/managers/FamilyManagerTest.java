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
import java.util.Set;
import java.util.stream.Collectors;

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

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);

        long projectId = catalogManager.getProjectManager().create("Project about some genomes", "1000G", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(String.valueOf(projectId), "Phase 1", "phase1", Study.Type.TRIO, null, "Done", null, null, null, null, null, null, null, null, sessionIdUser);

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void createFamily() throws CatalogException {
        QueryResult<Family> familyQueryResult = createDummyFamily();

        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(5, familyQueryResult.first().getMembers().size());
        assertEquals(2, familyQueryResult.first().getDiseases().size());

        boolean motherIdUpdated = false;
        boolean fatherIdUpdated = false;
        for (Relatives relatives : familyQueryResult.first().getMembers()) {
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

    public QueryResult<Family> createDummyFamily() throws CatalogException {
        OntologyTerm disease1 = new OntologyTerm("dis1", "Disease 1", "HPO");
        OntologyTerm disease2 = new OntologyTerm("dis2", "Disease 2", "HPO");

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual fatherChildren = new Individual().setName("father");
        Individual motherChildren = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), Arrays.asList("dis2"), null, false);
        Relatives relMother = new Relatives(mother, null, null, Arrays.asList("dis2"), Collections.emptyList(), null, false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), fatherChildren, motherChildren,
                Arrays.asList("dis1", "dis2"), Collections.emptyList(), new Multiples("multiples", Arrays.asList("child2", "child3")),
                true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), fatherChildren, motherChildren, Arrays.asList("dis1"),
                Collections.emptyList(), new Multiples("multiples", Arrays.asList("child1", "child3")), true);
        Relatives relChild3 = new Relatives(new Individual().setName("child3"), fatherChildren, motherChildren, Arrays.asList("dis1"),
                Collections.emptyList(), new Multiples("multiples", Arrays.asList("child1", "child2")), true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother),"", Collections.emptyList(), Collections.emptyMap());

        return familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyMissingMultiple() throws CatalogException {
        OntologyTerm disease1 = new OntologyTerm("dis1", "Disease 1", "HPO");
        OntologyTerm disease2 = new OntologyTerm("dis2", "Disease 2", "HPO");

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual fatherChildren = new Individual().setName("father");
        Individual motherChildren = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), Arrays.asList("dis2"), null, false);
        Relatives relMother = new Relatives(mother, null, null, Arrays.asList("dis2"), Collections.emptyList(), null, false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), fatherChildren, motherChildren,
                Arrays.asList("dis1", "dis2"), Collections.emptyList(), new Multiples("multiples", Arrays.asList("child2", "child3")),
                true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), fatherChildren, motherChildren, Arrays.asList("dis1"),
                Collections.emptyList(), new Multiples("multiples", Arrays.asList("child1", "child3")), true);
        Relatives relChild3 = new Relatives(new Individual().setName("child3"), fatherChildren, motherChildren, Arrays.asList("dis1"),
                Collections.emptyList(), new Multiples("multiples", Arrays.asList("child1")), true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not match");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyMissingMultiple2() throws CatalogException {
        OntologyTerm disease1 = new OntologyTerm("dis1", "Disease 1", "HPO");
        OntologyTerm disease2 = new OntologyTerm("dis2", "Disease 2", "HPO");

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual fatherChildren = new Individual().setName("father");
        Individual motherChildren = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), Arrays.asList("dis2"), null, false);
        Relatives relMother = new Relatives(mother, null, null, Arrays.asList("dis2"), Collections.emptyList(), null, false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), fatherChildren, motherChildren,
                Arrays.asList("dis1", "dis2"), Collections.emptyList(), new Multiples("multiples", Arrays.asList("child2", "child3")),
                true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), fatherChildren, motherChildren, Arrays.asList("dis1"),
                Collections.emptyList(), new Multiples("multiples", Arrays.asList("child1", "child3")), true);
        Relatives relChild3 = new Relatives(new Individual().setName("child3"), fatherChildren, motherChildren, Arrays.asList("dis1"),
                Collections.emptyList(), new Multiples("multiples", Arrays.asList("child1", "child20")), true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not match");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyMissingMember() throws CatalogException {
        OntologyTerm disease1 = new OntologyTerm("dis1", "Disease 1", "HPO");
        OntologyTerm disease2 = new OntologyTerm("dis2", "Disease 2", "HPO");

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), Collections.emptyList(), null, false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), father, mother, Arrays.asList("dis1", "dis2"),
                Collections.emptyList(), null, true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), father, mother, Arrays.asList("dis1"),
                Collections.emptyList(), null, true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relFather, relChild1, relChild2),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing family member");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyDiseaseNotPassed() throws CatalogException {
        OntologyTerm disease1 = new OntologyTerm("dis1", "Disease 1", "HPO");
        OntologyTerm disease2 = new OntologyTerm("dis2", "Disease 2", "HPO");

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), Collections.emptyList(), null, false);
        Relatives relMother = new Relatives(mother, null, null, Arrays.asList("dis2"), Collections.emptyList(), null, false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), father, mother, Arrays.asList("dis1", "dis3"),
                Collections.emptyList(), null, true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), father, mother, Arrays.asList("dis1"),
                Collections.emptyList(), null, true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relFather, relMother, relChild1, relChild2),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing disease");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyRepeatedMember() throws CatalogException {
        OntologyTerm disease1 = new OntologyTerm("dis1", "Disease 1", "HPO");
        OntologyTerm disease2 = new OntologyTerm("dis2", "Disease 2", "HPO");

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), Collections.emptyList(), null, false);
        Relatives relMother = new Relatives(mother, null, null, Arrays.asList("dis2"), Collections.emptyList(), null, false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), father, mother, Arrays.asList("dis1", "dis2"),
                Collections.emptyList(), null, true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), father, mother, Arrays.asList("dis1"),
                Collections.emptyList(), null, true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relFather, relMother, relChild1, relChild2, relChild1),"", Collections.emptyList(), Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Multiple members with same name");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updateFamilyMembers() throws CatalogException, JsonProcessingException {
        QueryResult<Family> originalFamily = createDummyFamily();

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother2");

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual fatherChildren = new Individual().setName("father");
        Individual motherChildren = new Individual().setName("mother2");

        // I do it this way to really leave diseases as null
        Relatives relFather = new Relatives().setMember(father);
        Relatives relMother = new Relatives().setMember(mother);
        Relatives relChild1 = new Relatives(new Individual().setName("child3"), fatherChildren, motherChildren,
                Arrays.asList("dis1", "dis2"), Collections.emptyList(), null, true);

        Family family = new Family();
        family.setMembers(Arrays.asList(relChild1, relFather, relMother));
        ObjectMapper jsonObjectMapper = catalogManagerResource.generateNewObjectMapper();

        ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(family));
        params = new ObjectMap(FamilyDBAdaptor.QueryParams.MEMBERS.key(), params.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()));

        QueryResult<Family> updatedFamily = familyManager.update(STUDY, originalFamily.first().getName(), params, QueryOptions.empty(),
                sessionIdUser);

        assertEquals(3, updatedFamily.first().getMembers().size());
        // Other parameters from the family should not have been stored in the database
        assertEquals(null, updatedFamily.first().getMembers().get(0).getMember().getName());

        // We store the ids when the family was first created
        Set<Long> originalFamilyIds = originalFamily.first().getMembers().stream()
                .map(m -> m.getMember().getId())
                .collect(Collectors.toSet());

        // Only one id should be the same as in originalFamilyIds (father id)
        for (Relatives relatives : updatedFamily.first().getMembers()) {
            if (relatives.getFather().getId() > 0) {
                assertTrue(originalFamilyIds.contains(relatives.getFather().getId()));
            }
            if (relatives.getMother().getId() > 0) {
                assertTrue(!originalFamilyIds.contains(relatives.getMother().getId()));
            }
        }
    }

    @Test
    public void updateFamilyMissingMember() throws CatalogException, JsonProcessingException {
        QueryResult<Family> originalFamily = createDummyFamily();

        Individual father = new Individual().setName("father");

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual fatherChildren = new Individual().setName("father");
        Individual motherChildren = new Individual().setName("mother2");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), Arrays.asList("dis2"), null, false);
        Relatives relChild1 = new Relatives(new Individual().setName("child3"), fatherChildren, motherChildren,
                Arrays.asList("dis1", "dis2"), Collections.emptyList(), null, true);

        Family family = new Family();
        family.setMembers(Arrays.asList(relChild1, relFather));
        ObjectMapper jsonObjectMapper = catalogManagerResource.generateNewObjectMapper();

        ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(family));
        params = new ObjectMap(FamilyDBAdaptor.QueryParams.MEMBERS.key(), params.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing family");
        familyManager.update(STUDY, originalFamily.first().getName(), params, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updateFamilyDisease() throws JsonProcessingException, CatalogException {
        QueryResult<Family> originalFamily = createDummyFamily();

        OntologyTerm disease1 = new OntologyTerm("dis1", "New name", "New source");
        OntologyTerm disease2 = new OntologyTerm("dis2", "New name", "New source");
        OntologyTerm disease3 = new OntologyTerm("dis3", "New name", "New source");

        Family family = new Family();
        family.setDiseases(Arrays.asList(disease1, disease2, disease3));
        ObjectMapper jsonObjectMapper = catalogManagerResource.generateNewObjectMapper();

        ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(family));
        params = new ObjectMap(FamilyDBAdaptor.QueryParams.DISEASES.key(), params.get(FamilyDBAdaptor.QueryParams.DISEASES.key()));

        QueryResult<Family> updatedFamily = familyManager.update(STUDY, originalFamily.first().getName(), params, QueryOptions.empty(),
                sessionIdUser);

        assertEquals(3, updatedFamily.first().getDiseases().size());

        // Only one id should be the same as in originalFamilyIds (father id)
        for (OntologyTerm disease : updatedFamily.first().getDiseases()) {
            assertEquals("New name", disease.getName());
            assertEquals("New source", disease.getSource());
        }
    }

    @Test
    public void updateFamilyMissingDisease() throws JsonProcessingException, CatalogException {
        QueryResult<Family> originalFamily = createDummyFamily();

        OntologyTerm disease1 = new OntologyTerm("dis1", "New name", "New source");

        Family family = new Family();
        family.setDiseases(Arrays.asList(disease1));
        ObjectMapper jsonObjectMapper = catalogManagerResource.generateNewObjectMapper();

        ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(family));
        params = new ObjectMap(FamilyDBAdaptor.QueryParams.DISEASES.key(), params.get(FamilyDBAdaptor.QueryParams.DISEASES.key()));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing disease");
        familyManager.update(STUDY, originalFamily.first().getName(), params, QueryOptions.empty(), sessionIdUser);
    }

}
