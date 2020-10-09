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

package org.opencb.opencga.app.cli.admin.executors;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.master.monitor.MonitorService;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;

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
        String subCommandString = catalogCommandOptions.getParsedSubCommand();
        logger.debug("Executing catalog admin {} command line", subCommandString);
        switch (subCommandString) {
            case "install":
                install();
                break;
            case "delete":
                delete();
                break;
            case "index":
                index();
                break;
            case "export":
                export();
                break;
            case "import":
                importDatabase();
                break;
            case "daemon":
                daemons();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void export() throws CatalogException {
        AdminCliOptionsParser.ExportCatalogCommandOptions commandOptions = catalogCommandOptions.exportCatalogCommandOptions;
        validateConfiguration(commandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
        String token = catalogManager.getUserManager().loginAsAdmin(adminPassword).getToken();

        if (StringUtils.isNotEmpty(commandOptions.project)) {
            catalogManager.getProjectManager().exportReleases(commandOptions.project, commandOptions.release, commandOptions.outputDir, token);
        } else if (StringUtils.isNotEmpty(commandOptions.study) && StringUtils.isNotEmpty(commandOptions.inputFile)) {
            catalogManager.getProjectManager().exportByFileNames(commandOptions.study, Paths.get(commandOptions.outputDir).toFile(),
                    Paths.get(commandOptions.inputFile).toFile(), token);
        }
    }

    private void importDatabase() throws CatalogException, IOException {
        AdminCliOptionsParser.ImportCatalogCommandOptions commandOptions = catalogCommandOptions.importCatalogCommandOptions;
        validateConfiguration(commandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
        String token = catalogManager.getUserManager().loginAsAdmin(adminPassword).getToken();

        catalogManager.getProjectManager().importReleases(commandOptions.owner, commandOptions.directory, token);
    }

    private void install() throws CatalogException, URISyntaxException {
        AdminCliOptionsParser.InstallCatalogCommandOptions commandOptions = catalogCommandOptions.installCatalogCommandOptions;

        validateConfiguration(commandOptions);

        this.configuration.getAdmin().setAlgorithm("HS256");

        this.configuration.getAdmin().setSecretKey(commandOptions.secretKey);
        if (StringUtils.isEmpty(configuration.getAdmin().getSecretKey())) {
            configuration.getAdmin().setSecretKey(RandomStringUtils.randomAlphabetic(16));
        }

        if (StringUtils.isEmpty(commandOptions.commonOptions.adminPassword)) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        CatalogManager catalogManager = new CatalogManager(configuration);
        if (catalogManager.existsCatalogDB()) {
            if (commandOptions.force) {
                // The password of the old db should match the one to be used in the new installation. Otherwise, they can obtain the same
                // results calling first to "catalog delete" and then "catalog install"
                String token = catalogManager.getUserManager().loginAsAdmin(commandOptions.commonOptions.adminPassword).getToken();
                catalogManager.deleteCatalogDB(token);
            } else {
                throw new CatalogException("A database called " + catalogManager.getCatalogDatabase() + " already exists");
            }
        }

        logger.info("\nInstalling database {} in {}\n", catalogManager.getCatalogDatabase(),
                configuration.getCatalog().getDatabase().getHosts());

        catalogManager.installCatalogDB(configuration.getAdmin().getSecretKey(), commandOptions.commonOptions.adminPassword,
                commandOptions.email, commandOptions.organization);
    }

    private void delete() throws CatalogException, URISyntaxException {
        validateConfiguration(catalogCommandOptions.deleteCatalogCommandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
        String token = catalogManager.getUserManager()
                .loginAsAdmin(catalogCommandOptions.deleteCatalogCommandOptions.commonOptions.adminPassword).getToken();

        logger.info("\nDeleting database {} from {}\n", catalogManager.getCatalogDatabase(), configuration.getCatalog().getDatabase()
                .getHosts());
        catalogManager.deleteCatalogDB(token);
    }

    private void index() throws CatalogException {
        validateConfiguration(catalogCommandOptions.indexCatalogCommandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
        String token = catalogManager.getUserManager()
                .loginAsAdmin(catalogCommandOptions.indexCatalogCommandOptions.commonOptions.adminPassword).getToken();

        logger.info("\nChecking and installing non-existing indexes in {} in {}\n",
                catalogManager.getCatalogDatabase(), configuration.getCatalog().getDatabase().getHosts());

        catalogManager.installIndexes(token);
    }

    private void daemons() throws Exception {
        validateConfiguration(catalogCommandOptions.daemonCatalogCommandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
        String token = catalogManager.getUserManager()
                .loginAsAdmin(catalogCommandOptions.daemonCatalogCommandOptions.commonOptions.adminPassword).getToken();

        if (catalogCommandOptions.daemonCatalogCommandOptions.start) {
            // Server crated and started
            MonitorService monitorService =
                    new MonitorService(configuration, appHome, token);
            monitorService.start();
            monitorService.blockUntilShutdown();
            logger.info("Shutting down OpenCGA Storage REST server");
        }

        if (catalogCommandOptions.daemonCatalogCommandOptions.stop) {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target("http://localhost:" + configuration.getMonitor().getPort())
                    .path("opencga")
                    .path("monitor")
                    .path("admin")
                    .path("stop");
            Response response = target.request().get();
            logger.info(response.toString());
        }
    }

    private void validateConfiguration(AdminCliOptionsParser.CatalogDatabaseCommandOptions catalogOptions) {
        if (catalogOptions.databaseUser != null) {
            configuration.getCatalog().getDatabase().setUser(catalogOptions.databaseUser);
        }
        if (catalogOptions.databasePassword != null) {
            configuration.getCatalog().getDatabase().setPassword(catalogOptions.databasePassword);
        }
        if (catalogOptions.prefix != null) {
            configuration.setDatabasePrefix(catalogOptions.prefix);
        }
        if (catalogOptions.databaseHost != null) {
            configuration.getCatalog().getDatabase().setHosts(Collections.singletonList(catalogOptions.databaseHost));
        }
    }
}
