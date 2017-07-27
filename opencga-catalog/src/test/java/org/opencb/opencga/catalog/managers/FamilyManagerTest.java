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
import org.opencb.opencga.catalog.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Family;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Study;

import java.io.IOException;
import java.util.Arrays;
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

        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, null);
        sessionIdUser = catalogManager.login("user", PASSWORD, "127.0.0.1").first().getId();

        long projectId = catalogManager.getProjectManager().create("Project about some genomes", "1000G", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.createStudy(projectId, "Phase 1", "phase1", Study.Type.TRIO, "Done", sessionIdUser);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void createFamily() throws CatalogException {
        Individual father = new Individual().setName("John").setSex(Individual.Sex.MALE);
        Individual mother = new Individual().setName("Sue").setSex(Individual.Sex.FEMALE);
        List<Individual> children = Arrays.asList(
                new Individual().setName("son"), new Individual().setName("daughter")
        );
        Family family = new Family("family", father, mother, children, false, "", 1);

        QueryResult<Family> familyQueryResult = familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, familyQueryResult.getNumResults());
        assertEquals("John", familyQueryResult.first().getFather().getName());
        assertEquals("Sue", familyQueryResult.first().getMother().getName());
        assertEquals(2, familyQueryResult.first().getChildren().size());

        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, FamilyDBAdaptor.QueryParams.CHILDREN.key());
        Query query = new Query(FamilyDBAdaptor.QueryParams.MOTHER.key(), "Sue");
        QueryResult<Family> search = familyManager.search(STUDY, query, options, sessionIdUser);
        assertEquals(null, search.first().getChildren());
        assertEquals("Sue", search.first().getMother().getName());
        assertEquals("John", search.first().getFather().getName());

        options = new QueryOptions(QueryOptions.EXCLUDE, FamilyDBAdaptor.QueryParams.FATHER.key());
        search = familyManager.search(STUDY, query, options, sessionIdUser);
        assertEquals(2, search.first().getChildren().size());
        assertEquals("Sue", search.first().getMother().getName());
        assertEquals(null, search.first().getFather());

        options = new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.FATHER.key());
        search = familyManager.search(STUDY, query, options, sessionIdUser);
        assertEquals(null, search.first().getChildren());
        assertEquals(null, search.first().getMother());
        assertEquals("John", search.first().getFather().getName());
    }
}
