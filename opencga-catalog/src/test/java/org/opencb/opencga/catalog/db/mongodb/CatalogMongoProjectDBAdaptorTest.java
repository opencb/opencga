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
        System.out.println(catalogProjectDBAdaptor.createProject(user1.getId(), p, null));
        p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", new Status(), "", 3000, "");
        System.out.println(catalogProjectDBAdaptor.createProject(user1.getId(), p, null));
        p = new Project("Project management project", "pmp", "yesterday", "it is a system", new Status(), "", 2000, "");
        System.out.println(catalogProjectDBAdaptor.createProject(user2.getId(), p, null));
        System.out.println(catalogProjectDBAdaptor.createProject(user1.getId(), p, null));

        try {
            System.out.println(catalogProjectDBAdaptor.createProject(user1.getId(), p, null));
            fail("Expected \"projectAlias already exists\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getProjectIdTest() throws CatalogDBException {
        assertTrue(catalogProjectDBAdaptor.getProjectId(user3.getId(), user3.getProjects().get(0).getAlias()) != -1);
        assertTrue(catalogProjectDBAdaptor.getProjectId(user3.getId(), "nonExistingProject") == -1);
    }


    @Test
    public void getProjectTest() throws CatalogDBException {
        long projectId = catalogProjectDBAdaptor.getProjectId(user3.getId(), user3.getProjects().get(0).getAlias());
        System.out.println("projectId = " + projectId);
        QueryResult<Project> project = catalogProjectDBAdaptor.getProject(projectId, null);
        System.out.println(project);
        assertNotNull(project.first());

        thrown.expect(CatalogDBException.class);    //"Expected \"bad id\" exception"
        catalogProjectDBAdaptor.getProject(-100, null);
    }

    @Test
    public void deleteProjectTest() throws CatalogException {
        Project p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", new Status(), "", 3000, "");
        QueryResult<Project> result = catalogProjectDBAdaptor.createProject(user1.getId(), p, null);
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
        QueryResult<Project> allProjects = catalogProjectDBAdaptor.getAllProjects(user3.getId(), null);
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
        Project p1 = catalogProjectDBAdaptor.createProject(user1.getId(), new Project("project1", "p1", "Tomorrow", "Cool", new Status(),
                "", 3000, ""), null).first();
        Project p2 = catalogProjectDBAdaptor.createProject(user1.getId(), new Project("project2", "p2", "Tomorrow", "Cool", new Status(),
                "", 3000, ""), null).first();
        System.out.println(catalogProjectDBAdaptor.renameProjectAlias(p1.getId(), "newpmp"));

        try {
            System.out.println(catalogProjectDBAdaptor.renameProjectAlias(-1, "falseProject"));
            fail("renamed project with projectId=-1");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
        try {
            System.out.println(catalogProjectDBAdaptor.renameProjectAlias(p1.getId(), p2.getAlias()));
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

    @Test
    public void projectAclTest() throws CatalogDBException {
        long projectId = user3.getProjects().get(0).getId();
        List<AclEntry> acls = catalogProjectDBAdaptor.getProjectAcl(projectId, user3.getId()).getResult();
        assertTrue(acls.isEmpty());
        acls = catalogProjectDBAdaptor.getProjectAcl(projectId, user2.getId()).getResult();
        assertTrue(acls.isEmpty());
        acls = catalogProjectDBAdaptor.getProjectAcl(projectId, "noUser").getResult();
        assertTrue(acls.isEmpty());


        AclEntry granted = new AclEntry("jmmut", true, true, true, false);
        System.out.println(catalogProjectDBAdaptor.setProjectAcl(projectId, granted));  // overwrites
        AclEntry jmmut = catalogProjectDBAdaptor.getProjectAcl(projectId, "jmmut").first();
        System.out.println(jmmut);
        assertTrue(jmmut.equals(granted));

        granted.setUserId("imedina");
        System.out.println(catalogProjectDBAdaptor.setProjectAcl(projectId, granted));  // just pushes
        AclEntry imedina = catalogProjectDBAdaptor.getProjectAcl(projectId, "imedina").first();
        System.out.println(imedina);
        assertTrue(imedina.equals(granted));
        try {
            granted.setUserId("noUser");
            catalogProjectDBAdaptor.setProjectAcl(projectId, granted);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }


}
