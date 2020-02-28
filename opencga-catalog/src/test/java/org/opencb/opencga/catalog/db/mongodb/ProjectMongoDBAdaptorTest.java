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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.project.DataStores;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectInternal;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
public class ProjectMongoDBAdaptorTest extends MongoDBAdaptorTest {

    @Test
    public void createProjectTest() throws CatalogException, JsonProcessingException {
        Project p = new Project("1000G", "Project about some genomes", "Today", "Cool", null, 1, new ProjectInternal(new DataStores(), new Status()));
        System.out.println(catalogProjectDBAdaptor.insert(p, user1.getId(), null));
        p = new Project("2000G", "Project about some more genomes", "Tomorrow", "Cool", null, 1, new ProjectInternal(new DataStores(), new Status()));
        System.out.println(catalogProjectDBAdaptor.insert(p, user1.getId(), null));
        p = new Project("pmp", "Project management project", "yesterday", "it is a system", null, 1, new ProjectInternal(new DataStores(), new Status()));
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
        DataResult<Project> projectDataResult = catalogProjectDBAdaptor.get(projectId, new QueryOptions(QueryOptions.INCLUDE,
                ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()));
        assertEquals(1, projectDataResult.first().getCurrentRelease());

        catalogProjectDBAdaptor.incrementCurrentRelease(projectId);
        int currentRelease = catalogProjectDBAdaptor.get(projectId, QueryOptions.empty()).first().getCurrentRelease();
        assertEquals(2, currentRelease);

        catalogProjectDBAdaptor.incrementCurrentRelease(projectId);
        currentRelease = catalogProjectDBAdaptor.get(projectId, QueryOptions.empty()).first().getCurrentRelease();
        assertEquals(3, currentRelease);
    }


    @Test
    public void getProjectTest() throws CatalogDBException {
        long projectId = catalogProjectDBAdaptor.getId(user3.getId(), user3.getProjects().get(0).getId());
        System.out.println("projectId = " + projectId);
        DataResult<Project> project = catalogProjectDBAdaptor.get(projectId, null);
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
        DataResult<Project> allProjects = catalogProjectDBAdaptor.get(user3.getId(), null);
        System.out.println(allProjects);
        assertTrue(!allProjects.getResults().isEmpty());
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
        catalogProjectDBAdaptor.insert(new Project("p1", "project1", null, "Cool", null, 1,
                new ProjectInternal(new DataStores(), new Status())), user1.getId(), null);
        Project p1 = getProject(user1.getId(), "p1");
        catalogProjectDBAdaptor.insert(new Project("p2", "project2", null, "Cool", null, 1,
                new ProjectInternal(new DataStores(), new Status())), user1.getId(), null);
        Project p2 = getProject(user1.getId(), "p2");

        catalogProjectDBAdaptor.update(p1.getUid(), new ObjectMap(ProjectDBAdaptor.QueryParams.ID.key(), "newpmp"), QueryOptions.empty());

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

}
