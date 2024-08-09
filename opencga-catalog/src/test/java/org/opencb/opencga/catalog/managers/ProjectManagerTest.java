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
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 28/11/16.
 */
@Category(MediumTests.class)
public class ProjectManagerTest extends AbstractManagerTest {

    @Test
    public void searchProjectByStudy() throws CatalogException {
        OpenCGAResult<Project> result = catalogManager.getProjectManager().search(organizationId, new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), "phase1"), null, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(project1, result.first().getId());
    }

    @Test
    public void getOwnProjectNoStudies() throws CatalogException {
        DataResult<Project> projectDataResult = catalogManager.getProjectManager().get(project3, null, orgAdminToken2);
        assertEquals(1, projectDataResult.getNumResults());
    }

    @Test
    public void getOtherUsersProject() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("cannot view");
        catalogManager.getProjectManager().get(project2, null, noAccessToken1);
    }

    @Test
    public void searchSampleNoPermissions() throws CatalogException {
        // First, remove anonymous user access to the study
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Collections.singletonList(ParamConstants.ANONYMOUS_USER_ID)), ownerToken);

        // User1 looks for any sample without providing any project or study and he has not been granted permissions anywhere
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("cannot view any study");
        catalogManager.getSampleManager().search("", new Query(), QueryOptions.empty(), noAccessToken1);
    }

    @Test
    public void searchProjects() throws CatalogException {
        String org2 = "otherOrg";
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(org2), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("userFromOrg2").setName("name").setOrganization(org2),
                TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getOrganizationManager().update(org2, new OrganizationUpdateParams().setOwner("userFromOrg2"), null, opencgaToken);
        String owner2Token = catalogManager.getUserManager().login(org2, "userFromOrg2", TestParamConstants.PASSWORD).getToken();
        Project p = catalogManager.getProjectManager().create(new ProjectCreateParams()
                        .setId("project")
                        .setOrganism(new ProjectOrganism("Homo sapiens", "GRCh38")),
                INCLUDE_RESULT, owner2Token).first();
        Study study = catalogManager.getStudyManager().create(p.getFqn(), new Study().setId("study"), INCLUDE_RESULT, owner2Token).first();

        catalogManager.getUserManager().create("userid", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, org2, null, owner2Token);
        String token = catalogManager.getUserManager().login(org2, "userid", TestParamConstants.PASSWORD).getToken();
        OpenCGAResult<Project> projectOpenCGAResult = catalogManager.getProjectManager().search(org2, new Query(), QueryOptions.empty(), token);
        assertTrue(projectOpenCGAResult.getResults().isEmpty());
        assertEquals(0, projectOpenCGAResult.getEvents().size());

        String otherUser = "user_tmp";
        catalogManager.getUserManager().create(otherUser, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, org2, null, owner2Token);
        String otherUsertoken = catalogManager.getUserManager().login(org2, otherUser, TestParamConstants.PASSWORD).getToken();
        OpenCGAResult<Project> result = catalogManager.getProjectManager()
                .search(org2, new Query(), QueryOptions.empty(), otherUsertoken);
        assertTrue(result.getResults().isEmpty());
        assertEquals(0, result.getEvents().size());

        // Give some permissions to the user in the study
        catalogManager.getStudyManager().updateGroup(study.getFqn(), "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(otherUser)), owner2Token);

        result = catalogManager.getProjectManager().search(org2, new Query(), QueryOptions.empty(), otherUsertoken);
        assertEquals(1, result.getNumResults());
        assertEquals(p.getId(), result.first().getId());
        assertEquals(p.getFqn(), result.first().getFqn());
    }

    @Test
    public void searchProjectsUsingInclude() throws CatalogException {
        OpenCGAResult<Project> projects = catalogManager.getProjectManager().search(organizationId, new Query(),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key()), ownerToken);
        assertEquals(3, projects.getNumResults());
        for (Project project : projects.getResults()) {
            assertNotNull(project.getId());
            assertNull(project.getDescription());
            assertNull(project.getName());
            assertNull(project.getStudies());
            assertTrue(CollectionUtils.isEmpty(project.getStudies()));
        }

        projects = catalogManager.getProjectManager().search(organizationId, new Query(),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.STUDIES.key()), ownerToken);
        assertEquals(3, projects.getNumResults());
        for (Project project : projects.getResults()) {
            assertNull(project.getDescription());
            assertNull(project.getName());
            assertNotNull(project.getStudies());
            if ("p1".equals(project.getId())) {
                assertTrue(CollectionUtils.isEmpty(project.getStudies()));
            } else {
                assertTrue(CollectionUtils.isNotEmpty(project.getStudies()));
            }
        }

        projects = catalogManager.getProjectManager().search(organizationId, new Query(),
                new QueryOptions(QueryOptions.EXCLUDE, ProjectDBAdaptor.QueryParams.NAME.key()), ownerToken);
        assertEquals(3, projects.getNumResults());
        for (Project project : projects.getResults()) {
            assertNotNull(project.getId());
            assertNull(project.getName());
            assertNotNull(project.getDescription());
            assertNotNull(project.getStudies());
        }
    }

    @Test
    public void updateOrganismInProject() throws CatalogException {
        Project pr = catalogManager.getProjectManager().create("project2", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, ownerToken).first();

        assertEquals("Homo sapiens", pr.getOrganism().getScientificName());
        assertEquals("", pr.getOrganism().getCommonName());
        assertEquals("GRCh38", pr.getOrganism().getAssembly());

        ObjectMap objectMap = new ObjectMap();
        objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_COMMON_NAME.key(), "common");

        OpenCGAResult<Project> update = catalogManager.getProjectManager().update(pr.getId(), objectMap, INCLUDE_RESULT, ownerToken);
        assertEquals(1, update.getNumResults());
        OpenCGAResult<Project> queryResult = catalogManager.getProjectManager().get(pr.getId(), null, ownerToken);

        assertEquals("Homo sapiens", queryResult.first().getOrganism().getScientificName());
        assertEquals("common", queryResult.first().getOrganism().getCommonName());
        assertEquals("GRCh38", queryResult.first().getOrganism().getAssembly());

        objectMap = new ObjectMap();
        objectMap.put(ProjectDBAdaptor.QueryParams.ORGANISM_ASSEMBLY.key(), "assembly");

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Cannot update organism");
        catalogManager.getProjectManager().update(pr.getId(), objectMap, null, ownerToken);
    }

    @Test
    public void createProjectCheckCellbase() throws CatalogException {
        Project pr = catalogManager.getProjectManager()
                .create(new ProjectCreateParams()
                                .setId("Project_1")
                                .setName("Project about some genomes")
                                .setOrganism(new ProjectOrganism("Homo sapiens", "grch38"))
                                .setCellbase(new CellBaseConfiguration(
                                        ParamConstants.CELLBASE_URL,
                                        "v5.0")),
                        INCLUDE_RESULT, ownerToken).first();
        assertNull(pr.getCellbase().getDataRelease());

        pr = catalogManager.getProjectManager()
                .create(new ProjectCreateParams()
                                .setId("Project_2")
                                .setName("Project about some genomes")
                                .setOrganism(new ProjectOrganism("Homo sapiens", "grch38"))
                                .setCellbase(new CellBaseConfiguration(
                                        ParamConstants.CELLBASE_URL,
                                        "5.0")),
                        INCLUDE_RESULT, ownerToken).first();
        assertNull(pr.getCellbase().getDataRelease());

        pr = catalogManager.getProjectManager()
                .create(new ProjectCreateParams()
                                .setId("Project_3")
                                .setName("Project about some genomes")
                                .setOrganism(new ProjectOrganism("Homo sapiens", "grch38"))
                                .setCellbase(new CellBaseConfiguration(
                                        ParamConstants.CELLBASE_URL,
                                        ParamConstants.CELLBASE_VERSION)),
                        INCLUDE_RESULT, ownerToken).first();
        assertNotNull(pr.getCellbase().getDataRelease());
    }

    @Test
    public void createProjectWrongCellbase() throws CatalogException, JsonProcessingException {
        thrown.expectMessage("DataRelease 'NON_EXISTING_DR' not found on cellbase");
        catalogManager.getProjectManager()
                .create(new ProjectCreateParams()
                                .setId("project2")
                                .setName("Project about some genomes")
                                .setOrganism(new ProjectOrganism("Homo sapiens", "grch38"))
                                .setCellbase(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, ParamConstants.CELLBASE_VERSION, "NON_EXISTING_DR", null)),
                        INCLUDE_RESULT, ownerToken).first();
    }

    @Test
    public void updateCellbaseInProject() throws CatalogException, JsonProcessingException {
        Project pr = catalogManager.getProjectManager().create("project2", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, ownerToken).first();
        assertNotNull(pr.getCellbase());
        assertEquals("https://ws.zettagenomics.com/cellbase", pr.getCellbase().getUrl());
        assertEquals(ParamConstants.CELLBASE_VERSION, pr.getCellbase().getVersion());

        CellBaseConfiguration cb = new CellBaseConfiguration("https://ws.opencb.org/cellbase", "v3");
        OpenCGAResult<Project> update = catalogManager.getProjectManager().setCellbaseConfiguration(pr.getId(),
                new CellBaseConfiguration("https://ws.opencb.org/cellbase", "v3"), false, ownerToken);
        assertEquals(1, update.getNumUpdated());

        Project project = catalogManager.getProjectManager().get(pr.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(pr.getCellbase());
        assertEquals(cb.getUrl(), project.getCellbase().getUrl());
        assertEquals(cb.getVersion(), project.getCellbase().getVersion());

        thrown.expectMessage("Unable to access cellbase url");
        catalogManager.getProjectManager().setCellbaseConfiguration(pr.getId(),
                new CellBaseConfiguration("https://ws.opencb.org/cellbase", "v3"), true, ownerToken);
    }
}
