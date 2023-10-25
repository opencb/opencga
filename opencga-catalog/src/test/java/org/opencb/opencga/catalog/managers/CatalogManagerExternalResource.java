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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.settings.SettingsCreateParams;
import org.opencb.opencga.core.models.user.User;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogManagerExternalResource extends ExternalResource {

    private static CatalogManager catalogManager;
    private Configuration configuration;
    private Path opencgaHome;
    protected String adminToken;
    protected String organizationId = "test";
    protected String ownerId = "user";
    protected String ownerToken;

    private static boolean firstExecutionFinished = false;

    public CatalogManagerExternalResource() {
        Configurator.setLevel("org.mongodb.driver.cluster", Level.WARN);
        Configurator.setLevel("org.mongodb.driver.connection", Level.WARN);
    }

    @Override
    public void before() throws Exception {
        int c = 0;
        do {
            opencgaHome = Paths.get("target/test-data").resolve("junit_opencga_home_" + TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (opencgaHome.toFile().exists());
        Files.createDirectories(opencgaHome);
        configuration = Configuration.load(getClass().getResource("/configuration-test.yml").openStream());
        configuration.setWorkspace(opencgaHome.resolve("sessions").toAbsolutePath().toString());
        configuration.setJobDir(opencgaHome.resolve("JOBS").toAbsolutePath().toString());

//        if (opencgaHome.toFile().exists()) {
//            deleteFolderTree(opencgaHome.toFile());
//            Files.createDirectory(opencgaHome);
//        }
        // Pedigree graph analysis
        Path analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/pedigree-graph")).toAbsolutePath();
        FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/pedigree-graph/ped.R");
        Files.copy(inputStream, analysisPath.resolve("ped.R"), StandardCopyOption.REPLACE_EXISTING);

        catalogManager = new CatalogManager(configuration);
        if (!firstExecutionFinished) {
            clearCatalog(configuration);
            firstExecutionFinished = true;

            String secretKey = PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH);
            catalogManager.installCatalogDB("HS256", secretKey, TestParamConstants.ADMIN_PASSWORD, "opencga@admin.com", true);

            adminToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
            catalogManager.getSettingsManager().create(new SettingsCreateParams("default", null, null), adminToken);
            catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"),
                    QueryOptions.empty(), adminToken);
            catalogManager.getUserManager().create(organizationId, new User().setId(ownerId).setName(ownerId), TestParamConstants.PASSWORD, adminToken);
            catalogManager.getOrganizationManager().update(organizationId, new OrganizationUpdateParams().setOwner(ownerId),
                    QueryOptions.empty(), adminToken);
//            dump(configuration);
        } else {
//            restoreDump(configuration);
            adminToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        }

        ownerToken = catalogManager.getUserManager().login(organizationId, ownerId, TestParamConstants.PASSWORD).getToken();
    }

    @Override
    public void after() {
        super.after();
        try {
            if (catalogManager != null) {
                catalogManager.close();
            }
        } catch (CatalogException e) {
            throw new RuntimeException(e);
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public String getAdminToken() {
        return "";
    }

    public Path getOpencgaHome() {
        return opencgaHome;
    }

    public ObjectMapper generateNewObjectMapper() {
        ObjectMapper jsonObjectMapper = getDefaultObjectMapper();
//        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        return jsonObjectMapper;
    }

    public void dump() throws CatalogDBException {
        StopWatch stopWatch = StopWatch.createStarted();
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(configuration)) {
            MongoClient mongoClient = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(dbAdaptorFactory.getOrganizationIds().get(0))
                    .getMongoDataStore().getMongoClient();
            MongoDatabase dumpDatabase = mongoClient.getDatabase("test_dump");
            dumpDatabase.drop();

            Bson emptyBsonQuery = new Document();
            Document databaseSummary = new Document();
            for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
                String databaseName = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId).getMongoDataStore().getDatabaseName();
                databaseSummary.put(organizationId, databaseName);

                MongoDatabase database = mongoClient.getDatabase(databaseName);
                for (String collection : OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST) {
                    MongoCollection<Document> dbCollection = database.getCollection(collection);
                    MongoCollection<Document> dumpCollection = dumpDatabase.getCollection(organizationId + "__" + collection);

                    try (MongoCursor<Document> iterator = dbCollection.find(emptyBsonQuery).noCursorTimeout(true).iterator()) {
                        List<Document> documentList = new LinkedList<>();
                        while (iterator.hasNext()) {
                            Document document = iterator.next();
                            if (OrganizationMongoDBAdaptorFactory.FILE_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.DELETED_FILE_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION.equals(collection)) {
                                // Store uri differently
                                String uri = document.getString("uri");
                                String temporalFolder = opencgaHome.getFileName().toString();
                                String replacedUri = uri.replace(temporalFolder, "TEMPORAL_FOLDER_HERE");
                                document.put("uri", replacedUri);
                            }
                            documentList.add(document);
                        }
                        if (!documentList.isEmpty()) {
                            dumpCollection.insertMany(documentList);
                        }
                    }
                }
            }
            dumpDatabase.getCollection("summary").insertOne(databaseSummary);
        }
        System.out.println("Database dump created in " + stopWatch.getTime(TimeUnit.MILLISECONDS) + " milliseconds.");
    }

    public void restoreDump() throws CatalogDBException, IOException, CatalogIOException {
        StopWatch stopWatch = StopWatch.createStarted();
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(configuration)) {
            MongoClient mongoClient = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(dbAdaptorFactory.getOrganizationIds().get(0))
                    .getMongoDataStore().getMongoClient();
            MongoDatabase dumpDatabase = mongoClient.getDatabase("test_dump");

            Bson emptyBsonQuery = new Document();
            for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
                String databaseName = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId).getMongoDataStore().getDatabaseName();
                MongoDatabase database = mongoClient.getDatabase(databaseName);

                // Write users folder in disk
                IOManager ioManager = catalogManager.getIoManagerFactory().getDefault();
                Path usersFolder = opencgaHome.resolve("sessions").resolve("orgs").resolve(organizationId).resolve("users");
                ioManager.createDirectory(usersFolder.toUri(), true);

                for (String collection : OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST) {
                    MongoCollection<Document> dbCollection = database.getCollection(collection);
                    MongoCollection<Document> dumpCollection = dumpDatabase.getCollection(organizationId + "__" + collection);

                    dbCollection.deleteMany(emptyBsonQuery);
                    try (MongoCursor<Document> iterator = dumpCollection.find(emptyBsonQuery).noCursorTimeout(true).iterator()) {
                        List<Document> documentList = new LinkedList<>();
                        while (iterator.hasNext()) {
                            Document document = iterator.next();
                            if (OrganizationMongoDBAdaptorFactory.FILE_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.DELETED_FILE_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION.equals(collection)) {

                                // Write actual temporal folder in database
                                String uri = document.getString("uri");
                                String temporalFolder = opencgaHome.getFileName().toString();
                                String replacedUri = uri.replace("TEMPORAL_FOLDER_HERE", temporalFolder);
                                document.put("uri", replacedUri);

                                if (OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)) {
                                    // Create temporal study folder
                                    ioManager.createDirectory(Paths.get(replacedUri).toUri(), true);
                                }
                            }
                            documentList.add(document);
                        }
                        if (!documentList.isEmpty()) {
                            dbCollection.insertMany(documentList);
                        }
                    }
                }
            }
        }
        System.out.println("Databases restored in " + stopWatch.getTime(TimeUnit.MILLISECONDS) + " milliseconds.");
    }

    public static void clearCatalog(Configuration configuration) throws CatalogException, URISyntaxException {
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(configuration)) {
            dbAdaptorFactory.deleteCatalogDB();
        }

//        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
//        for (String hostPort : configuration.getCatalog().getDatabase().getHosts()) {
//            if (hostPort.contains(":")) {
//                String[] split = hostPort.split(":");
//                Integer port = Integer.valueOf(split[1]);
//                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
//            } else {
//                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
//            }
//        }
//        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);
//
////        if (catalogManager == null) {
////            catalogManager = new CatalogManager(configuration);
////        }
//
////        MongoDataStore db = mongoManager.get(catalogConfiguration.getDatabase().getDatabase());
//        MongoDataStore db = mongoManager.get(configuration.getDatabasePrefix() + "_catalog");
//        db.getDb().drop();
////        mongoManager.close(catalogConfiguration.getDatabase().getDatabase());
//        mongoManager.close(configuration.getDatabasePrefix() + "_catalog");

        Path rootdir = Paths.get(UriUtils.createDirectoryUri(configuration.getWorkspace()));
        deleteFolderTree(rootdir.toFile());

        Path jobdir = Paths.get(UriUtils.createDirectoryUri(configuration.getJobDir()));
        deleteFolderTree(jobdir.toFile());
    }

    public static void deleteFolderTree(java.io.File folder) {
        java.io.File[] files = folder.listFiles();
        if (files != null) {
            for (java.io.File f : files) {
                if (f.isDirectory()) {
                    deleteFolderTree(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    public URI getResourceUri(String resourceName) throws IOException {
        return getResourceUri(resourceName, resourceName);
    }

    public URI getResourceUri(String resourceName, String targetName) throws IOException {
        Path resourcePath = opencgaHome.resolve("resources").resolve(targetName);
        if (!resourcePath.getParent().toFile().exists()) {
            Files.createDirectories(resourcePath.getParent());
        }
        if (!resourcePath.toFile().exists()) {
            try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream(resourceName)) {
                Assert.assertNotNull(resourceName, stream);
                Files.copy(stream, resourcePath, StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return resourcePath.toUri();
    }

}
