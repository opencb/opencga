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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.master.monitor.MonitorService;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
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
            case "status":
                status();
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
                break;//demo, dump , stats
        }
    }

    private void export() throws CatalogException {
        AdminCliOptionsParser.ExportCatalogCommandOptions commandOptions = catalogCommandOptions.exportCatalogCommandOptions;
        validateConfiguration(commandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
        String token = catalogManager.getUserManager().loginAsAdmin(adminPassword).getToken();

        if (StringUtils.isNotEmpty(commandOptions.project)) {
            catalogManager.getProjectManager().exportReleases(commandOptions.project, commandOptions.release, commandOptions.outputDir,
                    token);
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

        catalogManager.getProjectManager().importReleases(commandOptions.organizationId, commandOptions.owner, commandOptions.directory, token);
    }

    private void status() throws CatalogException, JsonProcessingException {
        AdminCliOptionsParser.StatusCatalogCommandOptions commandOptions = catalogCommandOptions.statusCatalogCommandOptions;
        validateConfiguration(commandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
//        if (StringUtils.isEmpty(commandOptions.commonOptions.adminPassword)) {
//            throw new CatalogException("No admin password found. Please, insert your password.");
//        }
        ObjectMap result = new ObjectMap();
        if (catalogManager.getDatabaseStatus()) {
            result.put("mongodbStatus", true);
            String adminDatabase = catalogManager.getCatalogAdminDatabase();
            result.put("catalogDBName", adminDatabase);
            result.put("catalogDBNames", catalogManager.getCatalogDatabaseNames());
            result.put("databasePrefix", catalogManager.getConfiguration().getDatabasePrefix());
            if (commandOptions.uri) {
                // check login
                catalogManager.getUserManager().loginAsAdmin(getAdminPassword(true));
                result.put("mongodbUri", MongoDBUtils.getMongoDBUri(configuration.getCatalog().getDatabase()));
                result.put("mongodbUriRedacted", MongoDBUtils.getMongoDBUriRedacted(configuration.getCatalog().getDatabase()));
                result.put("mongodbUriWithDatabase", MongoDBUtils.getMongoDBUri(
                        configuration.getCatalog().getDatabase(), adminDatabase));
                result.put("mongodbCliOpts", MongoDBUtils.getMongoDBCliOpts(configuration.getCatalog().getDatabase()));
                result.put("mongodbCli", MongoDBUtils.getMongoDBCli(
                        configuration.getCatalog().getDatabase(), adminDatabase));
            }
            if (catalogManager.existsCatalogDB()) {
                result.put("installed", true);
            } else {
                String oldDatabase = configuration.getDatabasePrefix() + "_catalog";
                MongoDBAdaptorFactory mongoDBAdaptorFactory = new MongoDBAdaptorFactory(configuration, catalogManager.getIoManagerFactory(),
                        catalogManager.getCatalogIOManager());
                MongoDataStore oldDatastore = mongoDBAdaptorFactory.getMongoManager().get(oldDatabase, mongoDBAdaptorFactory.getMongoDbConfiguration());
                try {
                    if (oldDatastore.getCollectionNames().contains("metadata")) {
                        Document metadata = oldDatastore.getCollection("metadata").find(new Document(), QueryOptions.empty()).first();
                        if (metadata != null) {
                            result.put("PENDING_3_0_0_MIGRATION", "Please, execute 'opencga-admin.sh migration v3.0.0' "
                                    + "to update your database to the latest version.");
//                            result.put("oldVersion", metadata.get("version"));

                            result.put("catalogDBName", oldDatabase);
                            result.put("catalogDBNames", Collections.singletonList(oldDatabase));

                            result.put("installed", true);
                        }
                    }
                } catch (Exception e) {
                    logger.debug("Error checking old database. Assume doesn't exist", e);
                }
                result.putIfAbsent("installed", false);
            }
        } else {
            result.put("mongodbStatus", false);
            result.put("installed", false);
        }
        System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result));
    }

    private void install() throws CatalogException {
        AdminCliOptionsParser.InstallCatalogCommandOptions commandOptions = catalogCommandOptions.installCatalogCommandOptions;

        validateConfiguration(commandOptions);

        if (StringUtils.isEmpty(commandOptions.commonOptions.adminPassword)) {
            throw new CatalogException("No admin password found. Please, insert your password.");
        }

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            catalogManager.installCatalogDB("HS256", commandOptions.secretKey, commandOptions.commonOptions.adminPassword,
                    commandOptions.email, commandOptions.force);
        }
    }

    private void delete() throws CatalogException {
        validateConfiguration(catalogCommandOptions.deleteCatalogCommandOptions);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            logger.info("Deleting databases {} from {}\n", catalogManager.getCatalogDatabaseNames(),
                    configuration.getCatalog().getDatabase().getHosts());
            catalogManager.deleteCatalogDB(catalogCommandOptions.deleteCatalogCommandOptions.commonOptions.adminPassword);
        }
    }

    private void index() throws CatalogException {
        validateConfiguration(catalogCommandOptions.indexCatalogCommandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
        String token = catalogManager.getUserManager()
                .loginAsAdmin(catalogCommandOptions.indexCatalogCommandOptions.commonOptions.adminPassword).getToken();

        String organizationId = catalogCommandOptions.indexCatalogCommandOptions.organizationId;
        if (StringUtils.isEmpty(organizationId)) {
            logger.info("Checking and installing non-existing indexes in {} fom {}\n",
                    catalogManager.getCatalogDatabaseNames(), configuration.getCatalog().getDatabase().getHosts());

            catalogManager.installIndexes(token);
        } else {
            logger.info("Checking and installing non-existing indexes in {} fom {}\n",
                    catalogManager.getCatalogDatabase(organizationId), configuration.getCatalog().getDatabase().getHosts());

            catalogManager.installIndexes(organizationId, token);
        }
    }

    private void daemons() throws Exception {
        validateConfiguration(catalogCommandOptions.daemonCatalogCommandOptions);

        CatalogManager catalogManager = new CatalogManager(configuration);
        String token = catalogManager.getUserManager()
                .loginAsAdmin(catalogCommandOptions.daemonCatalogCommandOptions.commonOptions.adminPassword).getToken();

        if (catalogCommandOptions.daemonCatalogCommandOptions.start) {
            // Server crated and started
            MonitorService monitorService =
                    new MonitorService(configuration, storageConfiguration, appHome, token);
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
