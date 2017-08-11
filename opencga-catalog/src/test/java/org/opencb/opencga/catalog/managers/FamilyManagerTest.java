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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
        Disease disease1 = new Disease("dis1", "Disease 1", Collections.emptyList());
        Disease disease2 = new Disease("dis2", "Disease 2", Collections.emptyList());

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), false);
        Relatives relMother = new Relatives(mother, null, null, Arrays.asList("dis2"), false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), father, mother, Arrays.asList("dis1", "dis2"), true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), father, mother, Arrays.asList("dis1"), true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relFather, relMother, relChild1, relChild2),"", 1, Collections.emptyMap());

        QueryResult<Family> familyQueryResult = familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals(4, familyQueryResult.first().getMembers().size());
        assertEquals(2, familyQueryResult.first().getDiseases().size());
    }

    @Test
    public void createFamilyMissingMember() throws CatalogException {
        Disease disease1 = new Disease("dis1", "Disease 1", Collections.emptyList());
        Disease disease2 = new Disease("dis2", "Disease 2", Collections.emptyList());

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), father, mother, Arrays.asList("dis1", "dis2"), true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), father, mother, Arrays.asList("dis1"), true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relFather, relChild1, relChild2),"", 1, Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing family member");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyDiseaseNotPassed() throws CatalogException {
        Disease disease1 = new Disease("dis1", "Disease 1", Collections.emptyList());
        Disease disease2 = new Disease("dis2", "Disease 2", Collections.emptyList());

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), false);
        Relatives relMother = new Relatives(mother, null, null, Arrays.asList("dis2"), false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), father, mother, Arrays.asList("dis1", "dis3"), true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), father, mother, Arrays.asList("dis1"), true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relFather, relMother, relChild1, relChild2),"", 1, Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing diseases");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createFamilyRepeatedMember() throws CatalogException {
        Disease disease1 = new Disease("dis1", "Disease 1", Collections.emptyList());
        Disease disease2 = new Disease("dis2", "Disease 2", Collections.emptyList());

        Individual father = new Individual().setName("father");
        Individual mother = new Individual().setName("mother");

        Relatives relFather = new Relatives(father, null, null, Arrays.asList("dis1"), false);
        Relatives relMother = new Relatives(mother, null, null, Arrays.asList("dis2"), false);
        Relatives relChild1 = new Relatives(new Individual().setName("child1"), father, mother, Arrays.asList("dis1", "dis2"), true);
        Relatives relChild2 = new Relatives(new Individual().setName("child2"), father, mother, Arrays.asList("dis1"), true);

        Family family = new Family("Martinez-Martinez", Arrays.asList(disease1, disease2),
                Arrays.asList(relFather, relMother, relChild1, relChild2, relChild1),"", 1, Collections.emptyMap());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Multiple members with same name");
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

}
