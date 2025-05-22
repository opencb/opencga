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

package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectInternal;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyInternal;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
@Category(MediumTests.class)
public class ProjectMongoDBAdaptorTest extends AbstractMongoDBAdaptorTest {

    @Test
    public void createProjectTest() throws CatalogException, JsonProcessingException {
        Project p = new Project("project1", "Project about some genomes", "", "", "Cool", null, 1, ProjectInternal.init())
                .setFqn(organizationId + ":project1");
        OpenCGAResult<Project> insert = catalogProjectDBAdaptor.insert(p, null);
        assertEquals(1, insert.getNumInserted());
        p = new Project("project2", "Project about some more genomes", "", "", "Cool", null, 1, ProjectInternal.init())
                .setFqn(organizationId + ":project2");
        insert = catalogProjectDBAdaptor.insert(p, null);
        assertEquals(1, insert.getNumInserted());
        p = new Project("project3", "Project management project", "", "", "it is a system", null, 1, ProjectInternal.init())
                .setFqn(organizationId + ":project3");
        insert = catalogProjectDBAdaptor.insert(p, null);
        assertEquals(1, insert.getNumInserted());

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("already exists");
        catalogProjectDBAdaptor.insert(p, null);
    }

    @Test
    public void incrementCurrentRelease() throws CatalogDBException {
        long projectUid = catalogProjectDBAdaptor.get(new Query(ProjectDBAdaptor.QueryParams.ID.key(), project1),
                QueryOptions.empty()).first().getUid();
        DataResult<Project> projectDataResult = catalogProjectDBAdaptor.get(projectUid, new QueryOptions(QueryOptions.INCLUDE,
                ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()));
        assertEquals(1, projectDataResult.first().getCurrentRelease());

        catalogProjectDBAdaptor.incrementCurrentRelease(projectUid);
        int currentRelease = catalogProjectDBAdaptor.get(projectUid, QueryOptions.empty()).first().getCurrentRelease();
        assertEquals(2, currentRelease);

        catalogProjectDBAdaptor.incrementCurrentRelease(projectUid);
        currentRelease = catalogProjectDBAdaptor.get(projectUid, QueryOptions.empty()).first().getCurrentRelease();
        assertEquals(3, currentRelease);
    }


    @Test
    public void getProjectTest() throws CatalogDBException {
        long projectUid = catalogProjectDBAdaptor.get(new Query(ProjectDBAdaptor.QueryParams.ID.key(), project1),
                QueryOptions.empty()).first().getUid();
        System.out.println("projectUid = " + projectUid);
        DataResult<Project> project = catalogProjectDBAdaptor.get(projectUid, null);
        System.out.println(project);
        assertNotNull(project.first());

        thrown.expect(CatalogDBException.class);    //"Expected \"bad id\" exception"
        catalogProjectDBAdaptor.get(-100, null);
    }

//    @Test
//    public void deleteProjectTest() throws CatalogException {
//        Project p = new Project("2000G", "Project about some more genomes", null, "Cool", new Status(), "", 3000, "", null, 1);
//        catalogProjectDBAdaptor.insert(p, user1.getId(), null);
//
//        p = getProject(user1.getId(), "2000G");
//        DataResult writeResult = catalogProjectDBAdaptor.delete(p.getUid(), new QueryOptions());
//        assertEquals(1, writeResult.getNumUpdated());
//
//        Query query = new Query(ProjectDBAdaptor.QueryParams.UID.key(), p.getUid())
//                .append(ProjectDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);
//        DataResult<Project> queryResult = catalogProjectDBAdaptor.get(query, QueryOptions.empty());
//        assertEquals(1, queryResult.getNumResults());
//        assertEquals(Status.DELETED, queryResult.first().getStatus().getName());
//
//        //thrown.expect(CatalogDBException.class);
//        //catalogProjectDBAdaptor.delete(p.getId());
//
//        thrown.expect(CatalogDBException.class);    //Expected "Project not found" exception
//        catalogProjectDBAdaptor.delete(-1, new QueryOptions());
//    }

    @Test
    public void getAllProjects() throws CatalogDBException {
        DataResult<Project> allProjects = catalogProjectDBAdaptor.get(new Query(), QueryOptions.empty());
        assertFalse(allProjects.getResults().isEmpty());
    }

    /**
     * cases:
     * ok: correct projectId, correct newName
     * error: non-existent projectId
     * error: newName already used
     * error: newName == oldName
     *
     * @throws CatalogDBException
     */
    @Test
    public void renameProjectTest() throws CatalogException {
        catalogProjectDBAdaptor.insert(new Project("myp1", "project1", null, null, "Cool", null, 1, ProjectInternal.init()).setFqn(FqnUtils.buildFqn(organizationId, "myp1")), null);
        Project p1 = getProject("myp1");
        catalogProjectDBAdaptor.insert(new Project("myp2", "project2", null, null, "Cool", null, 1, ProjectInternal.init()).setFqn(FqnUtils.buildFqn(organizationId, "myp2")), null);
        Project p2 = getProject("myp2");

        // Create project folder so the study insert doesn't fail
        CatalogIOManager catalogIOManager = new CatalogIOManager(catalogManager.getConfiguration());
        catalogIOManager.createProject(organizationId, Long.toString(p1.getUid()));

        // Add study
        catalogStudyDBAdaptor.insert(p1, new Study().setInternal(new StudyInternal()).setId("study").setFqn(FqnUtils.buildFqn(organizationId, "myp1", "study")), null);

        catalogProjectDBAdaptor.update(p1.getUid(), new ObjectMap(ProjectDBAdaptor.QueryParams.ID.key(), "newpmp"), QueryOptions.empty());
        Project project = getProject("newpmp");
        assertEquals(FqnUtils.buildFqn(organizationId, "newpmp"), project.getFqn());
        assertEquals(1, project.getStudies().size());
        assertEquals(FqnUtils.buildFqn(organizationId, "newpmp", "study"), project.getStudies().get(0).getFqn());

        try {
            catalogProjectDBAdaptor.update(-1, new ObjectMap(ProjectDBAdaptor.QueryParams.ID.key(), "falseProject"), QueryOptions.empty());
            fail("renamed project with projectId=-1");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
        try {
            catalogProjectDBAdaptor.update(p1.getUid(), new ObjectMap(ProjectDBAdaptor.QueryParams.ID.key(), p2.getId()), QueryOptions.empty());
            fail("renamed project with name collision");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void cellbaseUpdateTest() throws Exception {
        catalogProjectDBAdaptor.insert(new Project("myp1", "project1", null, null, "Cool", null, 1, ProjectInternal.init())
                .setFqn(organizationId + ":project1"), null);
        Project p1 = getProject("myp1");
        catalogProjectDBAdaptor.insert(new Project("myp2", "project2", null, null, "Cool", null, 1, ProjectInternal.init())
                .setFqn(organizationId + ":project2"), null);

        catalogProjectDBAdaptor.update(p1.getUid(), new ObjectMap(ProjectDBAdaptor.QueryParams.CELLBASE.key(),
                new CellBaseConfiguration("url", "v")), QueryOptions.empty());
        p1 = catalogProjectDBAdaptor.get(p1.getUid(), QueryOptions.empty()).first();
        assertEquals("url", p1.getCellbase().getUrl());
        assertEquals("v", p1.getCellbase().getVersion());
    }

}
