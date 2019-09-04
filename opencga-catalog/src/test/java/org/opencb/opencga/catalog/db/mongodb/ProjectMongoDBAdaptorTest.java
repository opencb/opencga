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

package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Status;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
public class ProjectMongoDBAdaptorTest extends MongoDBAdaptorTest {

    @Test
    public void createProjectTest() throws CatalogException, JsonProcessingException {
        Project p = new Project("1000G", "Project about some genomes", "Today", "Cool", new Status(), "", 1000, "", null, 1);
        System.out.println(catalogProjectDBAdaptor.insert(p, user1.getId(), null));
        p = new Project("2000G", "Project about some more genomes", "Tomorrow", "Cool", new Status(), "", 3000, "", null, 1);
        System.out.println(catalogProjectDBAdaptor.insert(p, user1.getId(), null));
        p = new Project("pmp", "Project management project", "yesterday", "it is a system", new Status(), "", 2000, "", null, 1);
        System.out.println(catalogProjectDBAdaptor.insert(p, user2.getId(), null));
        System.out.println(catalogProjectDBAdaptor.insert(p, user1.getId(), null));

        try {
            System.out.println(catalogProjectDBAdaptor.insert(p, user1.getId(), null));
            fail("Expected \"projectAlias already exists\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getProjectIdTest() throws CatalogDBException {
        assertTrue(catalogProjectDBAdaptor.getId(user3.getId(), user3.getProjects().get(0).getId()) != -1);
        assertTrue(catalogProjectDBAdaptor.getId(user3.getId(), "nonExistingProject") == -1);
    }

    @Test
    public void incrementCurrentRelease() throws CatalogDBException {
        long projectId = catalogProjectDBAdaptor.getId(user3.getId(), user3.getProjects().get(0).getId());
        QueryResult<Project> projectQueryResult = catalogProjectDBAdaptor.get(projectId, new QueryOptions(QueryOptions.INCLUDE,
                ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()));
        assertEquals(1, projectQueryResult.first().getCurrentRelease());

        assertEquals(2, catalogProjectDBAdaptor.incrementCurrentRelease(projectId).first().intValue());
        assertEquals(3, catalogProjectDBAdaptor.incrementCurrentRelease(projectId).first().intValue());
    }


    @Test
    public void getProjectTest() throws CatalogDBException {
        long projectId = catalogProjectDBAdaptor.getId(user3.getId(), user3.getProjects().get(0).getId());
        System.out.println("projectId = " + projectId);
        QueryResult<Project> project = catalogProjectDBAdaptor.get(projectId, null);
        System.out.println(project);
        assertNotNull(project.first());

        thrown.expect(CatalogDBException.class);    //"Expected \"bad id\" exception"
        catalogProjectDBAdaptor.get(-100, null);
    }

    @Test
    public void deleteProjectTest() throws CatalogException {
        Project p = new Project("2000G", "Project about some more genomes", null, "Cool", new Status(), "", 3000, "", null, 1);
        catalogProjectDBAdaptor.insert(p, user1.getId(), null);

        p = getProject(user1.getId(), "2000G");
        QueryResult<Project> queryResult = catalogProjectDBAdaptor.delete(p.getUid(), new QueryOptions());
        System.out.println(queryResult.first().getStatus());
        assertTrue(queryResult.getNumResults() == 1);

        //thrown.expect(CatalogDBException.class);
        //catalogProjectDBAdaptor.delete(p.getId());

        thrown.expect(CatalogDBException.class);    //Expected "Project not found" exception
        catalogProjectDBAdaptor.delete(-1, new QueryOptions());
    }

    @Test
    public void getAllProjects() throws CatalogDBException {
        QueryResult<Project> allProjects = catalogProjectDBAdaptor.get(user3.getId(), null);
        System.out.println(allProjects);
        assertTrue(!allProjects.getResult().isEmpty());
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
        catalogProjectDBAdaptor.insert(new Project("p1", "project1", null, "Cool", new Status(), "", 3000, "", null, 1), user1.getId(), null);
        Project p1 = getProject(user1.getId(), "p1");
        catalogProjectDBAdaptor.insert(new Project("p2", "project2", null, "Cool", new Status(), "", 3000, "", null, 1), user1.getId(), null);
        Project p2 = getProject(user1.getId(), "p2");

        catalogProjectDBAdaptor.editId(user1.getId(), p1.getUid(), "p1", "newpmp");

        try {
            catalogProjectDBAdaptor.editId(user1.getId(), -1, "", "falseProject");
            fail("renamed project with projectId=-1");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
        try {
            catalogProjectDBAdaptor.editId(user1.getId(), p1.getUid(), "newpmp", p2.getId());
            fail("renamed project with name collision");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

}
