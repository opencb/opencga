package org.opencb.opencga.catalog.core;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.Project;
import org.opencb.opencga.catalog.beans.User;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jacobo on 10/02/15.
 */
public class CatalogDBClientTest {

    private static CatalogManager catalogManager;
    private static String userId;
    private static String sessionId;
    private static CatalogClient catalog;

    @BeforeClass
    public static void beforeClass() throws IOException, CatalogException {
        Properties catalogProperties = new Properties();
        catalogProperties.load(CatalogDBClientTest.class.getClassLoader().getResourceAsStream("catalog.properties"));
        catalogManager = new CatalogManager(catalogProperties);
        QueryResult<ObjectMap> result = catalogManager.loginAsAnonymous("test-ip");
        userId = result.getResult().get(0).getString("userId");
        sessionId = result.getResult().get(0).getString("sessionId");

        catalog = new CatalogDBClient(catalogManager, sessionId);
    }

    @AfterClass
    public static void afterClass() throws CatalogException {
        catalog.close();
    }

    @Test
    public void connectionTest() throws CatalogException {
        QueryResult<User> read = catalog.users().read(null);
        System.out.println(read);
    }


    @Test
    public void modifyUserTest() throws CatalogException {
        QueryOptions attributes = new QueryOptions("myString", "asdf");
        attributes.add("myInt", 4);
        QueryOptions options = new QueryOptions("organization", "ACME");
        options.add("attributes", attributes);

        QueryResult<User> update = catalog.users(userId).update(options);
        System.out.println("update = " + update);
        System.out.println(catalog.users(userId).read(null));
    }

    @Test
    public void createProjectTest() throws CatalogException {
        QueryResult<Project> projectQueryResult = catalog.projects().create(userId, "project", "p1", "testProject", "ACME", null);
        System.out.println("projectQueryResult = " + projectQueryResult);
    }


}
