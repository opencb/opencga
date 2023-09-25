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

package com.zettagenomics.opencga.enterprise.cvdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.bson.Document;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngineIndexTest.INCLUDE_RESULT;
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
    private String adminToken;

    public static final String ADMIN_PASSWORD = "4dMiNiStR4t0R.";
    public static final String PASSWORD = "p4sSw0rD.";

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

        // Pedigree graph analysis
        Path analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/pedigree-graph")).toAbsolutePath();
        FileInputStream inputStream = new FileInputStream("../../opencga/opencga-app/app/analysis/pedigree-graph/ped.R");
        Files.copy(inputStream, analysisPath.resolve("ped.R"), StandardCopyOption.REPLACE_EXISTING);

        clearCatalog(configuration);
        if (!opencgaHome.toFile().exists()) {
            deleteFolderTree(opencgaHome.toFile());
            Files.createDirectory(opencgaHome);
        }
        configuration.getAdmin().setSecretKey(PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH));
        catalogManager = new CatalogManager(configuration);
        catalogManager.installCatalogDB(configuration.getAdmin().getSecretKey(), ADMIN_PASSWORD, "opencga@admin.com", "",
                true, true);
        catalogManager.close();
        // FIXME!! Should not need to create again the catalogManager
        //  Have to create again the CatalogManager, as it has a random "secretKey" inside
        catalogManager = new CatalogManager(configuration);
        adminToken = catalogManager.getUserManager().loginAsAdmin(ADMIN_PASSWORD).getToken();
    }

    @Override
    public void after() {
        super.after();
        try {
            if (catalogManager != null) {
                catalogManager.close();
            }
            adminToken = null;
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
        return adminToken;
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

    public static void clearCatalog(Configuration configuration) throws CatalogException, URISyntaxException {
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(configuration)) {
            for (String collection : MongoDBAdaptorFactory.COLLECTIONS_LIST) {
                dbAdaptorFactory.getMongoDataStore().getCollection(collection).remove(new Document(), QueryOptions.empty());
            }
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

    public void loadClinicalAnalsysesInCatalog(List<String> caFilenames, String studyId, String sessionIdUser)
            throws IOException, CatalogException {
        for (String caFilename : caFilenames) {
            InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream(caFilename);
            GZIPInputStream gzipInputStream = new GZIPInputStream(is);
            ClinicalAnalysis clinicalAnalysis = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class)
                    .readValue(gzipInputStream);

            // Import panels
            try {
                List<String> panelIds = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getPanels())) {
                    panelIds = clinicalAnalysis.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList());
                }
                catalogManager.getPanelManager().importFromSource(studyId, "panelapp", StringUtils.join(panelIds, ","), sessionIdUser);
            } catch (CatalogException e) {
                System.out.println("---------------------------------------------------------------------------------");
                System.out.println("Impossible to load clinical analysis file " + caFilename + ": " + e.getMessage());
                System.out.println("---------------------------------------------------------------------------------");
                continue;
            }

            // Create family
            if (clinicalAnalysis.getFamily() != null) {
                catalogManager.getFamilyManager().create(studyId, clinicalAnalysis.getFamily(), INCLUDE_RESULT, sessionIdUser);
            }

            // Create clinical analysis
            clinicalAnalysis.getInterpretation().setId(null);
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                    secondaryInterpretation.setId(null);
                }
            }
            catalogManager.getClinicalAnalysisManager().create(studyId, clinicalAnalysis, true, INCLUDE_RESULT, sessionIdUser);
        }
    }

}
