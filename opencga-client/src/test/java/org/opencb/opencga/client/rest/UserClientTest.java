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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * Created by imedina on 04/05/16.
 */
public class UserClientTest extends WorkEnvironmentTest {

    private UserClient userClient;

    @Before
    public void before() throws Throwable {
        super.before();
        userClient = openCGAClient.getUserClient();
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