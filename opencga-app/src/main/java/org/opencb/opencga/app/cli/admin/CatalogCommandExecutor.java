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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.analysis.demo.AnalysisDemo;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.CatalogDemo;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 02/03/15.
 */
public class CatalogCommandExecutor extends AdminCommandExecutor {

    private AdminCliOptionsParser.CatalogCommandOptions catalogCommandOptions;

    public CatalogCommandExecutor(AdminCliOptionsParser.CatalogCommandOptions catalogCommandOptions) {
        super(catalogCommandOptions.commonOptions);
        this.catalogCommandOptions = catalogCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = catalogCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "demo":
                demo();
                break;
            case "install":
                install();
                break;
            case "delete":
                delete();
                break;
            case "index":
                index();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void demo() throws CatalogException, StorageManagerException, IOException, URISyntaxException {
        if (catalogCommandOptions.demoCatalogCommandOptions.database != null) {
            catalogConfiguration.getDatabase().setDatabase(catalogCommandOptions.demoCatalogCommandOptions.database);
        } else {
            catalogConfiguration.getDatabase().setDatabase("opencga_catalog_demo");
        }
        catalogConfiguration.setOpenRegister(true);
        catalogConfiguration.getAdmin().setPassword("demo");
        CatalogDemo.createDemoDatabase(catalogConfiguration, catalogCommandOptions.demoCatalogCommandOptions.force);
        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        sessionId = catalogManager.login("user1", "user1_pass", "localhost").first().getString("sessionId");
        AnalysisDemo.insertPedigreeFile(catalogManager, 6L, Paths.get(this.appHome).resolve("examples/20130606_g1k.ped"), sessionId);
    }

    private void install() throws CatalogException {
        if (catalogCommandOptions.installCatalogCommandOptions.databaseUser != null) {
            catalogConfiguration.getDatabase().setUser(catalogCommandOptions.installCatalogCommandOptions.databaseUser);
        }
        if (catalogCommandOptions.installCatalogCommandOptions.databasePassword != null) {
            catalogConfiguration.getDatabase().setPassword(catalogCommandOptions.installCatalogCommandOptions.databasePassword);
        }
        if (catalogCommandOptions.installCatalogCommandOptions.database != null) {
            catalogConfiguration.getDatabase().setDatabase(catalogCommandOptions.installCatalogCommandOptions.database);
        }
        if (catalogCommandOptions.installCatalogCommandOptions.databaseHost != null) {
            catalogConfiguration.getDatabase().setHosts(Collections.singletonList(catalogCommandOptions.installCatalogCommandOptions.databaseHost));
        }
        if (catalogCommandOptions.commonOptions.adminPassword != null) {
            catalogConfiguration.getAdmin().setPassword(catalogCommandOptions.commonOptions.adminPassword);
        }

        if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        System.out.println("\nInstalling database " + catalogConfiguration.getDatabase().getDatabase() + " in "
                + catalogConfiguration.getDatabase().getHosts() + "\n");

//        if (!catalogCommandOptions.installCatalogCommandOptions.overwrite) {
//            if (checkDatabaseExists()) {
//                throw new CatalogException("The database " + catalogConfiguration.getDatabase().getDatabase() + " already exists.");
//            }
//        }

        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        catalogManager.installCatalogDB();
    }

    /**
     * Checks if the database exists.
     *
     * @return true if exists.
     */
    private boolean checkDatabaseExists() {
        List<DataStoreServerAddress> dataStoreServerAddresses = new ArrayList<>();
        for (String host : catalogConfiguration.getDatabase().getHosts()) {
            if (host.contains(":")) {
                String[] split = host.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(host, 27017));
            }
        }
        MongoDataStoreManager mongoDataStoreManager = new MongoDataStoreManager(dataStoreServerAddresses);
        return mongoDataStoreManager.exists(catalogConfiguration.getDatabase().getDatabase());
    }

    private void delete() throws CatalogException {
        if (catalogCommandOptions.deleteCatalogCommandOptions.databaseUser != null) {
            catalogConfiguration.getDatabase().setUser(catalogCommandOptions.deleteCatalogCommandOptions.databaseUser);
        }
        if (catalogCommandOptions.deleteCatalogCommandOptions.databasePassword != null) {
            catalogConfiguration.getDatabase().setPassword(catalogCommandOptions.deleteCatalogCommandOptions.databasePassword);
        }
        if (catalogCommandOptions.deleteCatalogCommandOptions.database != null) {
            catalogConfiguration.getDatabase().setDatabase(catalogCommandOptions.deleteCatalogCommandOptions.database);
        }
        if (catalogCommandOptions.deleteCatalogCommandOptions.databaseHost != null) {
            catalogConfiguration.getDatabase().setHosts(Collections.singletonList(catalogCommandOptions.deleteCatalogCommandOptions.databaseHost));
        }
        if (catalogCommandOptions.commonOptions.adminPassword != null) {
            catalogConfiguration.getAdmin().setPassword(catalogCommandOptions.commonOptions.adminPassword);
        }

        if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        if (!checkDatabaseExists()) {
            throw new CatalogException("The database " + catalogConfiguration.getDatabase().getDatabase() + " does not exist.");
        }
        System.out.println("\nDeleting " + catalogConfiguration.getDatabase().getDatabase() + " from "
                + catalogConfiguration.getDatabase().getHosts() + "\n");

        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        catalogManager.deleteCatalogDB(false);
    }

    private void index() throws CatalogException {
        if (catalogCommandOptions.indexCatalogCommandOptions.databaseUser != null) {
            catalogConfiguration.getDatabase().setUser(catalogCommandOptions.indexCatalogCommandOptions.databaseUser);
        }
        if (catalogCommandOptions.indexCatalogCommandOptions.databasePassword != null) {
            catalogConfiguration.getDatabase().setPassword(catalogCommandOptions.indexCatalogCommandOptions.databasePassword);
        }
        if (catalogCommandOptions.indexCatalogCommandOptions.database != null) {
            catalogConfiguration.getDatabase().setDatabase(catalogCommandOptions.indexCatalogCommandOptions.database);
        }
        if (catalogCommandOptions.indexCatalogCommandOptions.databaseHost != null) {
            catalogConfiguration.getDatabase().setHosts(Collections.singletonList(catalogCommandOptions.indexCatalogCommandOptions.databaseHost));
        }
        if (catalogCommandOptions.commonOptions.adminPassword != null) {
            catalogConfiguration.getAdmin().setPassword(catalogCommandOptions.commonOptions.adminPassword);
        }

        if (catalogConfiguration.getAdmin().getPassword() == null || catalogConfiguration.getAdmin().getPassword().isEmpty()) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        if (!checkDatabaseExists()) {
            throw new CatalogException("The database " + catalogConfiguration.getDatabase().getDatabase() + " does not exist.");
        }

        System.out.println("\nChecking and installing non-existent indexes in" + catalogConfiguration.getDatabase().getDatabase() + " in "
                + catalogConfiguration.getDatabase().getHosts() + "\n");

        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        catalogManager.installIndexes();
    }

}
