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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.GroupParams;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Study;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by pfurio on 28/11/16.
 */
public class ProjectManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    protected String sessionIdUser;
    protected String sessionIdUser2;
    protected String sessionIdUser3;
    private String project1;
    private String project2;
    private String project3;
    private String studyId;
    private String studyId2;
    private String studyId3;

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, Account.Type.FULL, null);

        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);
        sessionIdUser2 = catalogManager.getUserManager().login("user2", PASSWORD);
        sessionIdUser3 = catalogManager.getUserManager().login("user3", PASSWORD);

        project1 = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        project2 = catalogManager.getProjectManager().create("pmp", "Project Management Project", "life art intelligent system",
                "myorg", "Homo sapiens", null, null, "GRCh38", new QueryOptions(), sessionIdUser2).first().getId();
        project3 = catalogManager.getProjectManager().create("p1", "project 1", "", "", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser3).first().getId();

        studyId = catalogManager.getStudyManager().create(project1, "phase1", null, "Phase 1", Study.Type.TRIO, null, "Done", null, null, null, null, null, null, null, null, sessionIdUser).first().getFqn();
        studyId2 = catalogManager.getStudyManager().create(project1, "phase3", null, "Phase 3", Study.Type.CASE_CONTROL, null, "d", null, null, null, null, null, null, null, null, sessionIdUser).first().getFqn();

        studyId3 = catalogManager.getStudyManager().create(project2, "s1", null, "Study 1", Study.Type.CONTROL_SET, null, "", null, null, null, null, null, null, null, null, sessionIdUser2).first().getFqn();
    }

    @Test
    public void getOwnProjectNoStudies() throws CatalogException {
        DataResult<Project> projectDataResult = catalogManager.getProjectManager().get(project3, null, sessionIdUser3);
        assertEquals(1, projectDataResult.getNumResults());
    }

    @Test
    public void getSharedProjects() throws CatalogException {
        try {
            catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
            fail("User should not have permissions oveer this project yet");
        } catch (CatalogAuthorizationException e) {
            // Correct
        }

        // Create a new study in project2 with some dummy permissions for user
        String s2 = catalogManager.getStudyManager().create(project2, "s2", null, "Study 2", Study.Type.CONTROL_SET, null, "", null, null, null, null, null, null, null, null, sessionIdUser2).first().getId();
        catalogManager.getStudyManager().updateGroup(s2, "@members", new GroupParams("user", GroupParams.Action.ADD),
                sessionIdUser2);

        DataResult<Project> queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(1, queryResult.first().getStudies().size());
        assertEquals("s2", queryResult.first().getStudies().get(0).getId());

        // Add permissions to a group were user belongs
        catalogManager.getStudyManager().createGroup(studyId3, "@member", "@member", "user", sessionIdUser2);

        queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(2, queryResult.first().getStudies().size());
        assertEquals("user2@pmp", queryResult.first().getFqn());

        // Add permissions to user in a study of user3
        String s3 = catalogManager.getStudyManager().create(project3, "s3", null, "StudyProject3", Study.Type.CONTROL_SET, null, "", null, null, null, null, null, null, null, null, sessionIdUser3).first().getId();
        catalogManager.getStudyManager().updateGroup(String.valueOf(s3), "@members", new GroupParams("user", GroupParams.Action.ADD),
                sessionIdUser3);

        queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(2, queryResult.getNumResults());
        for (Project project : queryResult.getResults()) {
            if (project.getId().equals(project2)) {
                assertEquals(2, project.getStudies().size());
            } else {
                assertEquals(1, project.getStudies().size());
            }
        }
    }

    @Test
    public void updateOrganismInProject() throws CatalogException {
        Project pr = catalogManager.getProjectManager().create("project2", "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", null, sessionIdUser).first();

        assertEquals("Homo sapiens", pr.getOrganism().getScientificName());
        assertEquals("", pr.getOrganism().getCommonName());
        assertEquals("GRCh38", pr.getOrganism().getAssembly());
        assertEquals(-1, pr.getOrganism().getTaxonomyCode());

        ObjectMap objectMap = new ObjectMap();
        objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_TAXONOMY_CODE.key(), 55);
        DataResult<Project> update = catalogManager.getProjectManager().update(pr.getId(), objectMap, null, sessionIdUser);
        assertEquals(1, update.getNumResults());
        DataResult<Project> queryResult = catalogManager.getProjectManager().get(pr.getId(), null, sessionIdUser);

        assertEquals("Homo sapiens", queryResult.first().getOrganism().getScientificName());
        assertEquals("", queryResult.first().getOrganism().getCommonName());
        assertEquals("GRCh38", queryResult.first().getOrganism().getAssembly());
        assertEquals(55, queryResult.first().getOrganism().getTaxonomyCode());

        objectMap = new ObjectMap();
        objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(), "common");

        update = catalogManager.getProjectManager().update(pr.getId(), objectMap, null, sessionIdUser);
        assertEquals(1, update.getNumResults());
        queryResult = catalogManager.getProjectManager().get(pr.getId(), null, sessionIdUser);

        assertEquals("Homo sapiens", queryResult.first().getOrganism().getScientificName());
        assertEquals("common", queryResult.first().getOrganism().getCommonName());
        assertEquals("GRCh38", queryResult.first().getOrganism().getAssembly());
        assertEquals(55, queryResult.first().getOrganism().getTaxonomyCode());

        objectMap = new ObjectMap();
        objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(), "assembly");

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Cannot update organism");
        catalogManager.getProjectManager().update(pr.getId(), objectMap, null, sessionIdUser);
    }
}
