/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.config;

import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by imedina on 16/03/16.
 */
public class ConfigurationTest {

    @Test
    public void testDefault() {
        Configuration configuration = new Configuration();

        configuration.setLogLevel("INFO");

        configuration.setWorkspace("/opt/opencga/sessions");

        configuration.setAdmin(new Admin());

        Authentication authentication = new Authentication();
        configuration.setAuthentication(authentication);

        configuration.setMonitor(new Monitor());
        configuration.getAnalysis().setExecution(new Execution());

        configuration.setHooks(Collections.singletonMap("user@project:study", Collections.singletonMap("file",
                Collections.singletonList(
                        new HookConfiguration("name", "~*SV*", HookConfiguration.Stage.CREATE, HookConfiguration.Action.ADD, "tags", "SV")
        ))));

        List<AuthenticationOrigin> authenticationOriginList = new ArrayList<>();
        authenticationOriginList.add(new AuthenticationOrigin("opencga", AuthenticationOrigin.AuthenticationType.OPENCGA.toString(),
                "localhost", Collections.emptyMap()));
        Map<String, Object> myMap = new HashMap<>();
        myMap.put("ou", "People");
        authenticationOriginList.add(new AuthenticationOrigin("opencga", AuthenticationOrigin.AuthenticationType.LDAP.toString(),
                "ldap://10.10.0.20:389", myMap));
        configuration.getAuthentication().setAuthenticationOrigins(authenticationOriginList);

        Email emailServer = new Email("localhost", "", "", "", "", false);
        configuration.setEmail(emailServer);

        DatabaseCredentials databaseCredentials = new DatabaseCredentials(Arrays.asList("localhost"), "admin", "");
        Catalog catalog = new Catalog();
        catalog.setDatabase(databaseCredentials);
        configuration.setCatalog(catalog);

        Audit audit = new Audit("", 20000000, 100);
        configuration.setAudit(audit);

        ServerConfiguration serverConfiguration = new ServerConfiguration();
        RestServerConfiguration rest = new RestServerConfiguration(1000, 100, 1000);
        GrpcServerConfiguration grpc = new GrpcServerConfiguration(1001);
        serverConfiguration.setGrpc(grpc);
        serverConfiguration.setRest(rest);

        configuration.setServer(serverConfiguration);

//        CellBaseConfiguration cellBaseConfiguration = new CellBaseConfiguration(Arrays.asList("localhost"), "v3",
// new DatabaseCredentials(Arrays.asList("localhost"), "user", "password"));
//        QueryServerConfiguration queryServerConfiguration = new QueryServerConfiguration(61976, Arrays.asList("localhost"));
//
//        catalogConfiguration.setDefaultStorageEngineId("mongodb");
//
//        catalogConfiguration.setCellbase(cellBaseConfiguration);
//        catalogConfiguration.setServer(queryServerConfiguration);
//
//        catalogConfiguration.getStorageEngines().add(storageEngineConfiguration1);
//        catalogConfiguration.getStorageEngines().add(storageEngineConfiguration2);

        try {
            configuration.serialize(new FileOutputStream("/tmp/configuration-test.yml"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoad() throws Exception {
//        URL url = new URL("http://resources.opencb.org/opencb/opencga/disease-panels/sources.txt");
//        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), Charset.defaultCharset()));
//
//        Set<String> sources = new HashSet<>();
//        String line;
//        while((line = reader.readLine()) != null) {
//            sources.add(line);
//        }
//        System.out.println(sources);
//
//        File file = new File(url.toURI());
//        System.out.println(file.list());

        Configuration configuration = Configuration
                .load(getClass().getResource("/configuration-test.yml").openStream());
        System.out.println("catalogConfiguration = " + configuration);
    }
}