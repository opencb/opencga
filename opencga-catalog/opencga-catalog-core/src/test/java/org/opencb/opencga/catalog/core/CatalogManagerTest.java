package org.opencb.opencga.catalog.core;

import org.junit.*;
import static org.junit.Assert.*;
import org.junit.runners.MethodSorters;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.Project;
import org.opencb.opencga.catalog.core.beans.Study;
import org.opencb.opencga.catalog.core.beans.User;
import org.opencb.opencga.catalog.core.db.CatalogManagerException;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@FixMethodOrder(MethodSorters.JVM)
public class CatalogManagerTest extends GenericTest {

    public static final String PASSWORD = "asdf";
    static CatalogManager catalogManager;
    private String sessionIdUser;
    private String sessionIdUser2;

    @BeforeClass
    public static void init() throws IOException, CatalogIOManagerException {
        catalogManager = new CatalogManager("/tmp/opencga");
    }

    @Before
    public void setUp() throws IOException, CatalogIOManagerException {
        List<ObjectMap> result = null;
        try {
            result = catalogManager.login("user", PASSWORD, "127.0.0.1").getResult();
            sessionIdUser = result.get(0).getString("sessionId");
        } catch (CatalogManagerException | IOException ignore) {
        }
        try {
            result = catalogManager.login("user2", PASSWORD, "127.0.0.1").getResult();
            sessionIdUser2 = result.get(0).getString("sessionId");
        } catch (CatalogManagerException | IOException ignore) {
        }

    }

    @After
    public void tearDown() throws Exception {
        if(sessionIdUser != null) {
            catalogManager.logout("user", sessionIdUser);
        }
        if(sessionIdUser2 != null) {
            catalogManager.logout("user2", sessionIdUser2);
        }
    }


    @Test
    public void testCreateUser() throws Exception {
        User user = new User("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", "", "");
        System.out.println(catalogManager.createUser(user));
        user = new User("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", "", "");
        System.out.println(catalogManager.createUser(user));
    }

    @Test
    public void testLoginAsAnonymous() throws Exception {
        System.out.println(catalogManager.loginAsAnonymous("127.0.0.1"));
    }

    @Test
    public void testLogin() throws Exception {
        QueryResult<ObjectMap> queryResult = catalogManager.login("user", PASSWORD, "127.0.0.1");
        System.out.println(queryResult.getResult().get(0).toJson());
    }

    @Test
    public void testLogout() throws Exception {

    }

    @Test
    public void testLogoutAnonymous() throws Exception {
        QueryResult<ObjectMap> queryResult = catalogManager.loginAsAnonymous("127.0.0.1");
        catalogManager.logoutAnonymous(queryResult.getResult().get(0).getString("sessionId"));
    }

    @Test
    public void testGetUserInfo() throws CatalogManagerException {
        System.out.println(catalogManager.getUser("user", null, sessionIdUser));
        try {
            catalogManager.getUser("user", null, sessionIdUser2);
            fail();
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    @Test
    public void testModifyUser() throws CatalogManagerException {
        Map<String, String> options = new HashMap<>();
        options.put("name", "Changed Name");
        options.put("attributes.myAttribute", "5");
        options.put("attributes.myStruct.value.ok", "true");
        catalogManager.modifyUser("user", options, sessionIdUser);

        try {
            options = new HashMap();
            options.put("password", "1234321");
            catalogManager.modifyUser("user", options, sessionIdUser);
            fail("Expected exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

        try {
            catalogManager.modifyUser("user", options, sessionIdUser2);
            fail("Expected exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

    }

    @Test
    public void testCreateProject() throws Exception {
        Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
        System.out.println(catalogManager.createProject("user", p, sessionIdUser));
    }

    @Test
    public void testGetAllProjects() throws Exception {
        System.out.println(catalogManager.getAllProjects("user", sessionIdUser));
        System.out.println(catalogManager.getAllProjects("user", sessionIdUser2));
    }

    @Test
    public void testModifyProject() throws CatalogManagerException {
        Map<String, String> options = new HashMap<>();
        options.put("name", "new ProjectName");
        options.put("attributes.myAttribute", "5");
        options.put("attributes.myStruct.value.ok", "true");

        int projectId = catalogManager.getUser("user", null, sessionIdUser).getResult().get(0).getProjects().get(0).getId();
        catalogManager.modifyProject(projectId, options, sessionIdUser);

        try {
            options = new HashMap<>();
            options.put("alias", "newProjectalias");
            catalogManager.modifyProject(projectId, options, sessionIdUser);
            fail("Expected 'Parameter can't be changed' exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

        try {
            catalogManager.modifyProject(projectId, options, sessionIdUser2);
            fail("Expected 'Permission denied' exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

    }

    @Test
    public void testCreateStudy() throws Exception {
        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
        Study study;
        study = new Study("Phase 3", "phase3", "", "d", "");
        System.out.println(catalogManager.createStudy(projectId, study, sessionIdUser));
        study = new Study("Phase 1", "phase1", "", "Done", "");
        System.out.println(catalogManager.createStudy(projectId, study, sessionIdUser));
    }

    @Test
    public void testDeleteDataFromStudy() throws Exception {

    }

    @Test
    public void testCreateFolder() throws Exception {
        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
        int sessionId = catalogManager.getAllStudies(projectId, sessionIdUser).getResult().get(0).getId();
        System.out.println(catalogManager.createFolder(sessionId, Paths.get("data", "nueva", "carpeta"), true, sessionIdUser));
    }
}