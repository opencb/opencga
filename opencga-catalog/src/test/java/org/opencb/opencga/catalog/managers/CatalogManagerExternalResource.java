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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;
import static org.opencb.opencga.catalog.utils.ResourceManager.RESOURCES_DIRNAME;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogManagerExternalResource extends ExternalResource {

    protected CatalogManager catalogManager;
    protected Configuration configuration;
    protected Path opencgaHome;
    protected String adminToken;
    public boolean initialized = false;

    protected Path sourceAnalysisPath;

    public CatalogManagerExternalResource() {
        this(Paths.get("../opencga-app/app/analysis/"));
    }

    public CatalogManagerExternalResource(Path sourceAnalysisPath) {
        this.sourceAnalysisPath = sourceAnalysisPath;
        Configurator.setLevel("org.mongodb.driver.cluster", Level.WARN);
        Configurator.setLevel("org.mongodb.driver.connection", Level.WARN);
    }


    @Override
    public void before() throws Exception {
        initialized = true;
        System.out.println("-------------------------------------------------------------------------------");
        System.out.println("Initializing CatalogManagerExternalResource");
        System.out.println("-------------------------------------------------------------------------------");
        if (catalogManager != null) {
            catalogManager.close();
            catalogManager = null;
        }
        clearOpenCGAHome("static");

        //Catalog database might be already installed. Need to delete it before installing it again.
        clearCatalog(configuration);

        catalogManager = new CatalogManager(configuration);
        String secretKey = PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH);
        catalogManager.installCatalogDB("HS256", secretKey, TestParamConstants.ADMIN_PASSWORD, "opencga@admin.com", true);

        adminToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
    }

    public Path clearOpenCGAHome(String testName) throws IOException {
        int c = 0;
        do {
            opencgaHome = Paths.get("target/test-data").resolve("junit_opencga_home_" + testName + "_" + TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (opencgaHome.toFile().exists());
        Files.createDirectories(opencgaHome);
        configuration = Configuration.load(getClass().getResource("/configuration-test.yml").openStream());
        Path confPath = Files.createDirectories(opencgaHome.resolve("conf"));
        Files.copy(getClass().getResource("/configuration-test.yml").openStream(), confPath.resolve("configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        configuration.setWorkspace(opencgaHome.resolve("sessions").toAbsolutePath().toString());
        configuration.setJobDir(opencgaHome.resolve("JOBS").toAbsolutePath().toString());

        Path analysisPath = opencgaHome.resolve(ANALYSIS_DIRNAME);
        Files.createDirectories(analysisPath.resolve(RESOURCES_DIRNAME));

        // Pedigree graph analysis
        analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/pedigree-graph")).toAbsolutePath();
        InputStream inputStream = getClass().getResource("/pedigree-graph/ped.R").openStream();
        Files.copy(inputStream, analysisPath.resolve("ped.R"), StandardCopyOption.REPLACE_EXISTING);

        return opencgaHome;
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
        System.out.println("-------------------------------------------------------------------------------");
        System.out.println("Shutting down CatalogManagerExternalResource");
        System.out.println("-------------------------------------------------------------------------------");
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public CatalogManager resetCatalogManager() throws CatalogException {
        catalogManager.close();
        catalogManager = new CatalogManager(configuration);
        adminToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        return catalogManager;
    }

    public String getAdminToken() {
        return adminToken;
    }

    public Path getOpencgaHome() {
        return opencgaHome;
    }

    public static void clearCatalog(Configuration configuration) throws CatalogException, URISyntaxException {
        CatalogIOManager catalogIOManager = new CatalogIOManager(configuration);
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(configuration, new IOManagerFactory(), catalogIOManager)) {
            dbAdaptorFactory.deleteCatalogDB();
        }

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

    public String createTmpOutdir() throws IOException {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        // stackTrace[0] = "Thread.currentThread"
        // stackTrace[1] = "newOutputUri"
        // stackTrace[2] =  caller method
        String testName = stackTrace[2].getMethodName();
        return createTmpOutdir(testName);
    }

    public String createTmpOutdir(String suffix) throws IOException {
        if (suffix.endsWith("_")) {
            suffix = suffix.substring(0, suffix.length() - 1);
        }
        String folder = "I_tmp_" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss.SSS").format(new Date()) + suffix;
        Path tmpOutDir = Paths.get(getCatalogManager().getConfiguration().getJobDir()).resolve(folder);
        Files.createDirectories(tmpOutDir);
        return tmpOutDir.toString();
    }

}
