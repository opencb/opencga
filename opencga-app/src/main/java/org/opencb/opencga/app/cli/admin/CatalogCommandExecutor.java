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

package org.opencb.opencga.app.cli.admin;


import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 02/03/15.
 */
public class CatalogCommandExecutor extends CommandExecutor {

    private CliOptionsParser.CatalogCommandOptions catalogCommandOptions;

    public CatalogCommandExecutor(CliOptionsParser.CatalogCommandOptions catalogCommandOptions) {
        super(catalogCommandOptions.commonOptions);
        this.catalogCommandOptions = catalogCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = catalogCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "install":
                install();
                break;
            case "delete":
                install();
                break;
            case "index":
                index();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void install() throws CatalogException {
        if (catalogCommandOptions.installCatalogCommandOptions.databaseUser != null) {
            configuration.getDatabase().setUser(catalogCommandOptions.installCatalogCommandOptions.databaseUser);
        }
        if (catalogCommandOptions.installCatalogCommandOptions.databasePassword != null) {
            configuration.getDatabase().setPassword(catalogCommandOptions.installCatalogCommandOptions.databasePassword);
        }
        if (catalogCommandOptions.installCatalogCommandOptions.database != null) {
            configuration.getDatabase().setDatabase(catalogCommandOptions.installCatalogCommandOptions.database);
        }
        if (catalogCommandOptions.installCatalogCommandOptions.hosts != null) {
            configuration.getDatabase().setHosts(Collections.singletonList(catalogCommandOptions.installCatalogCommandOptions.hosts));
        }
        if (catalogCommandOptions.commonOptions.password != null) {
            configuration.getAdmin().setPassword(catalogCommandOptions.commonOptions.password);
        }

        if (configuration.getAdmin().getPassword() == null || configuration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        System.out.println("\nInstalling database " + configuration.getDatabase().getDatabase() + " in "
                + configuration.getDatabase().getHosts() + "\n");

        if (!catalogCommandOptions.installCatalogCommandOptions.overwrite) {
            checkDatabaseExists();
        }

        new CatalogManager(configuration);

    }

    private void checkDatabaseExists() throws CatalogException {
        List<DataStoreServerAddress> dataStoreServerAddresses = new ArrayList<>();
        for (String host : configuration.getDatabase().getHosts()) {
            if (host.contains(":")) {
                String[] split = host.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(host, 27017));
            }
        }
        MongoDataStoreManager mongoDataStoreManager = new MongoDataStoreManager(dataStoreServerAddresses);
        if (mongoDataStoreManager.exists(configuration.getDatabase().getDatabase())) {
            throw new CatalogException("The database " + configuration.getDatabase().getDatabase() + " already exists.");
        }

    }

    private void delete() {

    }

    private void index() {

    }

}
