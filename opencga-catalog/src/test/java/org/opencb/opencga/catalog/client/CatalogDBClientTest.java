/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.client;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;

import java.io.IOException;

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
        CatalogConfiguration catalogConfiguration = CatalogConfiguration.load(CatalogDBClientTest.class.getClassLoader().getClass()
                .getResource("/catalog-configuration-test.yml").openStream());
        /*
        Properties catalogProperties = new Properties();
        catalogProperties.load(CatalogDBClientTest.class.getClassLoader().getResourceAsStream("catalog.properties"));
        */
        catalogManager = new CatalogManager(catalogConfiguration);
        catalogManager.deleteCatalogDB(true);
        catalogManager.installCatalogDB();
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
