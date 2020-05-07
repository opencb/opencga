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

package org.opencb.opencga.client.rest;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.clients.UserClient;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.LoginParams;
import org.opencb.opencga.core.models.user.PasswordChangeParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

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
    public void login() throws ClientException {
        AuthenticationResponse response = openCGAClient.login("user1", "user1_pass");
        assertEquals(response.getToken(), openCGAClient.getToken());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Bad user or password");
        openCGAClient.login("user1", "wrong_password");
    }

    @Test
    public void logout() throws ClientException {
        System.out.println("token = " + userClient.login(new LoginParams("user1", "user1_pass"), null).firstResult().getToken());
        assertNotNull(openCGAClient.getToken());
        openCGAClient.logout();
        assertEquals(null, openCGAClient.getToken());
    }

    @Test
    public void get() throws Exception {
        RestResponse<User> login = userClient.info(null, null);
        assertNotNull(login.firstResult());
        assertEquals(1, login.allResultsSize());
    }

    @Test
    public void getProjects() throws Exception {
        RestResponse<Project> projects = userClient.projects(null, null);
        assertEquals(1, projects.allResultsSize());
    }


    @Test
    public void getProjectsLogout() throws IOException, ClientException {
        openCGAClient.logout();

        RestResponse<Project> projects = userClient.projects("user1", null);
        assertTrue(projects.getEvents().stream()
                .filter(event -> event.getType() == Event.Type.ERROR)
                .map(Event::getMessage)
                .collect(Collectors.joining()).contains("session id"));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing user id");
        userClient.projects(null, null);
    }

    @Test
    public void changePassword() throws Exception {
        userClient.password(new PasswordChangeParams("user1", "user1_pass", "user1_newPass"));
        String lastSessionId = openCGAClient.getToken();
        AuthenticationResponse response = openCGAClient.login(openCGAClient.getUserId(), "user1_newPass");
        assertNotEquals(lastSessionId, response.getToken());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Bad user or password");
        userClient.password(new PasswordChangeParams("user1", "wrongOldPassword", "anyPassword"));
    }

}