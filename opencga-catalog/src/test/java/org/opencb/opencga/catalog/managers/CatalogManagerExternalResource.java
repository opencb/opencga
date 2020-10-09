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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

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
        configuration.setJobDir(opencgaHome.resolve("jobs").toAbsolutePath().toString());

        clearCatalog(configuration);
        if (!opencgaHome.toFile().exists()) {
            deleteFolderTree(opencgaHome.toFile());
            Files.createDirectory(opencgaHome);
        }
        catalogManager = new CatalogManager(configuration);
        catalogManager.installCatalogDB("dummy", "admin", "opencga@admin.com", "", false);
        catalogManager.close();
        // FIXME!! Should not need to create again the catalogManager
        //  Have to create again the CatalogManager, as it has a random "secretKey" inside
        catalogManager = new CatalogManager(configuration);
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

    public Path getOpencgaHome() {
        return opencgaHome;
    }

    public ObjectMapper generateNewObjectMapper() {
        ObjectMapper jsonObjectMapper = getDefaultObjectMapper();
//        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        return jsonObjectMapper;
    }

    public static void clearCatalog(Configuration configuration) throws IOException, CatalogException, URISyntaxException {
        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : configuration.getCatalog().getDatabase().getHosts()) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);

//        if (catalogManager == null) {
//            catalogManager = new CatalogManager(configuration);
//        }

//        MongoDataStore db = mongoManager.get(catalogConfiguration.getDatabase().getDatabase());
        MongoDataStore db = mongoManager.get(configuration.getDatabasePrefix() + "_catalog");
        db.getDb().drop();
//        mongoManager.close(catalogConfiguration.getDatabase().getDatabase());
        mongoManager.close(configuration.getDatabasePrefix() + "_catalog");

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
}
