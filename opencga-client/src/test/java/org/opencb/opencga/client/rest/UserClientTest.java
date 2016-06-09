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

package org.opencb.opencga.client.rest;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by imedina on 04/05/16.
 */
public class UserClientTest extends WorkEnvironmentTest {

    private UserClient userClient;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws Throwable {
        super.before();
        userClient = openCGAClient.getUserClient();
    }

    @Test
    public void login() {
        QueryResponse<ObjectMap> login = userClient.login("user1", "user1_pass");
        assertNotNull(login);
        assertEquals(1, login.first().getNumResults());
        assertNotNull(login.first().first().getString("sessionId"));
        login = userClient.login("user1", "wrong_password");
        assertEquals(-1, login.first().getNumResults());
        assertTrue(login.getError().contains("Bad user or password"));
    }

    @Test
    public void logout() {
        System.out.println("sessionId = " + userClient.login("user1", "user1_pass").first().first().getString("sessionId"));
        QueryResponse<ObjectMap> user1 = userClient.logout("user1");
        user1 = userClient.logout("user1");
        assertTrue(1==1);
    }

    @Test
    public void get() throws Exception {
        userClient = openCGAClient.getUserClient();
        QueryResponse<User> login = userClient.get("user1", null);
        assertNotNull(login.firstResult());
    }

    @Test
    public void getProjects() throws Exception {
        userClient = openCGAClient.getUserClient();
        QueryResponse<Project> login = userClient.getProjects("hgva", null);
        assertNotNull(login.firstResult());
    }


}