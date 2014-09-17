package org.opencb.opencga.catalog.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.Project;
import org.opencb.opencga.catalog.core.beans.User;
import org.opencb.opencga.catalog.core.db.CatalogManagerException;

import java.util.List;

public class CatalogManagerTest extends GenericTest {

    public static final String PASSWORD = "asdf";
    CatalogManager catalogManager;
    private String sessionIdUser;
    private String sessionIdUser2;

    @Before
    public void setUp() throws Exception {
        catalogManager = new CatalogManager("/tmp/opencga");
        List<ObjectMap> result = catalogManager.login("user", PASSWORD, "127.0.0.1").getResult();
        if(!result.isEmpty()) {
            sessionIdUser = result.get(0).getString("sessionId");
        }
        result = catalogManager.login("user2", PASSWORD, "127.0.0.1").getResult();
        if(!result.isEmpty()) {
            sessionIdUser2 = result.get(0).getString("sessionId");
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
    public void testGetUserPath() throws Exception {

    }

    @Test
    public void testGetProjectPath() throws Exception {

    }

    @Test
    public void testGetFilePath() throws Exception {

    }

    @Test
    public void testGetTmpPath() throws Exception {

    }

    @Test
    public void testGetObjectFromBucket() throws Exception {

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
        System.out.println(catalogManager.getUserInfo("user", null, sessionIdUser));
        try {
            catalogManager.getUserInfo("user", null, sessionIdUser2);
            assert false;
        } catch (CatalogManagerException ignored) {
        }
    }

    @Test
    public void testGetAllProjects() throws Exception {
        System.out.println(catalogManager.getAllProjects("user", sessionIdUser));
        System.out.println(catalogManager.getAllProjects("user", sessionIdUser2));
    }

    @Test
    public void testCreateProject() throws Exception {
        Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
        System.out.println(catalogManager.createProject("user", p, sessionIdUser));
    }

    @Test
    public void testDeleteDataFromStudy() throws Exception {

    }
}