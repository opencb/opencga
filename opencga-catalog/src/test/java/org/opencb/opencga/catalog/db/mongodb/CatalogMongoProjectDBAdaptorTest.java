package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Status;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
public class CatalogMongoProjectDBAdaptorTest extends CatalogMongoDBAdaptorTest {

    @Test
    public void createProjectTest() throws CatalogException, JsonProcessingException {
        Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", new Status(), "", 1000, "");
        LinkedList<AclEntry> acl = new LinkedList<>();
        System.out.println(catalogProjectDBAdaptor.insert(p, user1.getId(), null));
        p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", new Status(), "", 3000, "");
        System.out.println(catalogProjectDBAdaptor.insert(p, user1.getId(), null));
        p = new Project("Project management project", "pmp", "yesterday", "it is a system", new Status(), "", 2000, "");
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
        assertTrue(catalogProjectDBAdaptor.getId(user3.getId(), user3.getProjects().get(0).getAlias()) != -1);
        assertTrue(catalogProjectDBAdaptor.getId(user3.getId(), "nonExistingProject") == -1);
    }


    @Test
    public void getProjectTest() throws CatalogDBException {
        long projectId = catalogProjectDBAdaptor.getId(user3.getId(), user3.getProjects().get(0).getAlias());
        System.out.println("projectId = " + projectId);
        QueryResult<Project> project = catalogProjectDBAdaptor.get(projectId, null);
        System.out.println(project);
        assertNotNull(project.first());

        thrown.expect(CatalogDBException.class);    //"Expected \"bad id\" exception"
        catalogProjectDBAdaptor.get(-100, null);
    }

    @Test
    public void deleteProjectTest() throws CatalogException {
        Project p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", new Status(), "", 3000, "");
        QueryResult<Project> result = catalogProjectDBAdaptor.insert(p, user1.getId(), null);
        System.out.println(result.first().getStatus());
        p = result.first();
        QueryResult<Project> queryResult = catalogProjectDBAdaptor.delete(p.getId(), new QueryOptions());
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
        Project p1 = catalogProjectDBAdaptor.insert(new Project("project1", "p1", "Tomorrow", "Cool", new Status(),
                "", 3000, ""), user1.getId(), null).first();
        Project p2 = catalogProjectDBAdaptor.insert(new Project("project2", "p2", "Tomorrow", "Cool", new Status(),
                "", 3000, ""), user1.getId(), null).first();
        System.out.println(catalogProjectDBAdaptor.renameAlias(p1.getId(), "newpmp"));

        try {
            System.out.println(catalogProjectDBAdaptor.renameAlias(-1, "falseProject"));
            fail("renamed project with projectId=-1");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
        try {
            System.out.println(catalogProjectDBAdaptor.renameAlias(p1.getId(), p2.getAlias()));
            fail("renamed project with name collision");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

//        try {
//            System.out.println(catalogUserDBAdaptor.renameProjectAlias(p1.getId(), p1.getAlias()));
//            fail("renamed project to its old name");
//        } catch (CatalogDBException e) {
//            System.out.println("correct exception: " + e);
//        }
    }

}
