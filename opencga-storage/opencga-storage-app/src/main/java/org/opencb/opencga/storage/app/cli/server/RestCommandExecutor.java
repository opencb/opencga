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

package org.opencb.opencga.storage.app.cli.server;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.server.rest.RestStorageServer;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 30/12/15.
 */
public class RestCommandExecutor extends CommandExecutor {

    private ServerCliOptionsParser.RestCommandOptions restCommandOptions;

    public RestCommandExecutor(ServerCliOptionsParser.RestCommandOptions restCommandOptions) {
        super(restCommandOptions.commonOptions);
        this.restCommandOptions = restCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing REST command line");

        String subCommandString = restCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "start":
                start();
                break;
            case "stop":
                stop();
                break;
            case "status":
                status();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    public void start() throws Exception {
        StorageConfiguration storageConfiguration = configuration;
        if (StringUtils.isNotEmpty(restCommandOptions.restStartCommandOptions.commonOptions.configFile)) {
            Path path = Paths.get(restCommandOptions.restStartCommandOptions.commonOptions.configFile);
            if (Files.exists(path)) {
                storageConfiguration = StorageConfiguration.load(Files.newInputStream(path));
            }
        }

        // Setting CLI params in the StorageConfiguration
        if (restCommandOptions.restStartCommandOptions.port > 0) {
            storageConfiguration.getServer().setRest(restCommandOptions.restStartCommandOptions.port);
        }

        if (StringUtils.isNotEmpty(restCommandOptions.restStartCommandOptions.commonOptions.storageEngine)) {
            storageConfiguration.setDefaultStorageEngineId(restCommandOptions.restStartCommandOptions.commonOptions.storageEngine);
        }

        if (StringUtils.isNotEmpty(restCommandOptions.restStartCommandOptions.authManager)) {
            storageConfiguration.getServer().setAuthManager(restCommandOptions.restStartCommandOptions.authManager);
        }

        // Server crated and started
        RestStorageServer server = new RestStorageServer(storageConfiguration);
        server.start();
        server.blockUntilShutdown();
        logger.info("Shutting down OpenCGA Storage REST server");
    }

    public void stop() {
        int port = configuration.getServer().getRest();
        if (restCommandOptions.restStopCommandOptions.port > 0) {
            port = restCommandOptions.restStopCommandOptions.port;
        }

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target("http://localhost:" + port)
                .path("opencga")
                .path("webservices")
                .path("rest")
                .path("admin")
                .path("stop");
        Response response = target.request().get();
        logger.info(response.toString());
    }

    public void status() {

    }

}
