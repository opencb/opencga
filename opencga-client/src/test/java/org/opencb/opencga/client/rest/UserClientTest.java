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

import org.junit.Test;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 04/05/16.
 */
public class UserClientTest {

    private UserClient userClient;
    private ClientConfiguration clientConfiguration;

    public UserClientTest() {
        try {
            clientConfiguration = ClientConfiguration.load(getClass().getResourceAsStream("/client-configuration-test.yml"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    @Before
//    public void setUp() throws Exception {
////        Path inputPath = Paths.get(getClass().getResource("/client-configuration-test.yml").toURI());
////        getClass().getResourceAsStream("/client-configuration-test.yml")
//        System.out.println();
//        userClient = new UserClient(clientConfiguration);
//    }

    @Test
    public void login() throws Exception {
        userClient = new UserClient(clientConfiguration);
        userClient.login("user", "pass");
    }

}