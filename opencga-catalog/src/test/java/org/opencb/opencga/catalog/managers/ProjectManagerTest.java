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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 28/11/16.
 */
public class ProjectManagerTest extends GenericTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    private String opencgaToken;
    protected String sessionIdUser;
    protected String sessionIdUser2;
    protected String sessionIdUser3;
    private String project1;
    private String project2;
    private String project3;
    private String studyId;
    private String studyId2;
    private String studyId3;

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", TestParamConstants.PASSWORD, "ACME", null, Account.AccountType.FULL, opencgaToken);

        sessionIdUser = catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD).getToken();
        sessionIdUser2 = catalogManager.getUserManager().login("user2", TestParamConstants.PASSWORD).getToken();
        sessionIdUser3 = catalogManager.getUserManager().login("user3", TestParamConstants.PASSWORD).getToken();

        project1 = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first().getId();
        project2 = catalogManager.getProjectManager().create("pmp", "Project Management Project", "life art intelligent system",
                "Homo sapiens", null, "GRCh38", INCLUDE_RESULT, sessionIdUser2).first().getId();
        project3 = catalogManager.getProjectManager().create("p1", "project 1", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser3).first().getId();

        studyId = catalogManager.getStudyManager().create(project1, "phase1", null, "Phase 1", "Done", null, null, null, null, INCLUDE_RESULT, sessionIdUser).first().getFqn();
        studyId2 = catalogManager.getStudyManager().create(project1, "phase3", null, "Phase 3", "d", null, null, null, null, INCLUDE_RESULT, sessionIdUser).first().getFqn();

        studyId3 = catalogManager.getStudyManager().create(project2, "s1", null, "Study 1", "", null, null, null, null, INCLUDE_RESULT, sessionIdUser2).first().getFqn();
    }

    @Test
    public void getOwnProjectNoStudies() throws CatalogException {
        DataResult<Project> projectDataResult = catalogManager.getProjectManager().get(project3, null, sessionIdUser3);
        assertEquals(1, projectDataResult.getNumResults());
    }

    @Test
    public void getOtherUsersProject() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("cannot view");
        catalogManager.getProjectManager().get(project1, null, sessionIdUser3);
    }

    @Test
    public void searchSampleNoPermissions() throws CatalogException {
        // User3 looks for any sample without providing any project or study and he has not been granted permissions anywhere
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("cannot view any study");
        catalogManager.getSampleManager().search("", new Query(), QueryOptions.empty(), sessionIdUser3);
    }

    @Test
    public void searchProjects() throws CatalogException {
        catalogManager.getUserManager().create("userid", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
        String token = catalogManager.getUserManager().login("userid", TestParamConstants.PASSWORD).getToken();
        OpenCGAResult<Project> projectOpenCGAResult = catalogManager.getProjectManager().search(new Query(), QueryOptions.empty(), token);
        assertTrue(projectOpenCGAResult.getResults().isEmpty());
        assertEquals(0, projectOpenCGAResult.getEvents().size());
    }

    @Test
    public void getSharedProjects() throws CatalogException {
        try {
            OpenCGAResult<Project> user = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        } catch (CatalogAuthorizationException e) {
            // Correct
        }

        // Create a new study in project2 with some dummy permissions for user
        String s2 = catalogManager.getStudyManager().create(project2, "s2", null, "Study 2", "", null, null, null, null, INCLUDE_RESULT, sessionIdUser2).first().getId();
        catalogManager.getStudyManager().updateGroup(s2, "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user")), sessionIdUser2);

        DataResult<Project> queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(1, queryResult.first().getStudies().size());
        assertEquals("s2", queryResult.first().getStudies().get(0).getId());

        // Add permissions to a group were user belongs
        catalogManager.getStudyManager().createGroup(studyId3, "@member", Collections.singletonList("user"), sessionIdUser2);

        queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(2, queryResult.first().getStudies().size());
        assertEquals("user2@pmp", queryResult.first().getFqn());

        // Add permissions to user in a study of user3
        String s3 = catalogManager.getStudyManager().create(project3, "s3", null, "StudyProject3", "", null, null, null, null, INCLUDE_RESULT, sessionIdUser3).first().getId();
        catalogManager.getStudyManager().updateGroup(String.valueOf(s3), "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user")), sessionIdUser3);

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
        Project pr = catalogManager.getProjectManager().create("project2", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first();

        assertEquals("Homo sapiens", pr.getOrganism().getScientificName());
        assertEquals("", pr.getOrganism().getCommonName());
        assertEquals("GRCh38", pr.getOrganism().getAssembly());

        ObjectMap objectMap = new ObjectMap();
        objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(), "common");

        OpenCGAResult<Project> update = catalogManager.getProjectManager().update(pr.getId(), objectMap, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, update.getNumResults());
        OpenCGAResult<Project> queryResult = catalogManager.getProjectManager().get(pr.getId(), null, sessionIdUser);

        assertEquals("Homo sapiens", queryResult.first().getOrganism().getScientificName());
        assertEquals("common", queryResult.first().getOrganism().getCommonName());
        assertEquals("GRCh38", queryResult.first().getOrganism().getAssembly());

        objectMap = new ObjectMap();
        objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(), "assembly");

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Cannot update organism");
        catalogManager.getProjectManager().update(pr.getId(), objectMap, null, sessionIdUser);
    }

    @Test
    public void updateCellbaseInProject() throws CatalogException, JsonProcessingException {
        Project pr = catalogManager.getProjectManager().create("project2", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first();
        assertNotNull(pr.getCellbase());
        assertEquals("https://ws.zettagenomics.com/cellbase", pr.getCellbase().getUrl());
        assertEquals(ParamConstants.CELLBASE_VERSION, pr.getCellbase().getVersion());

        CellBaseConfiguration cb = new CellBaseConfiguration("https://ws.opencb.org/cellbase", "v3");
        OpenCGAResult<Project> update = catalogManager.getProjectManager().setCellbaseConfiguration(pr.getId(),
                new CellBaseConfiguration("https://ws.opencb.org/cellbase", "v3"), sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        Project project = catalogManager.getProjectManager().get(pr.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(pr.getCellbase());
        assertEquals(cb.getUrl(), project.getCellbase().getUrl());
        assertEquals(cb.getVersion(), project.getCellbase().getVersion());
    }
}
