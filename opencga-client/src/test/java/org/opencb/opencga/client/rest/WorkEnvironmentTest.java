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

package org.opencb.opencga.client.rest;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.CatalogDemo;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.server.RestServer;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Created by pfurio on 09/06/16.
 */
public class WorkEnvironmentTest extends ExternalResource {

    protected OpenCGAClient openCGAClient;
    protected Path opencgaHome;
    protected CatalogManager catalogManager;
    protected ClientConfiguration clientConfiguration;
    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected RestServer restServer;

    @Override
    protected void before() throws Throwable {
        super.before();
        isolateOpenCGA();
    }

    private void isolateOpenCGA() throws Exception {
        opencgaHome = Paths.get("target/test-data").resolve("junit_opencga_home_" + RandomStringUtils.randomAlphabetic(10));
        Files.createDirectories(opencgaHome);
        storageConfiguration = StorageConfiguration.load(getClass().getResource("/storage-configuration.yml").openStream());
        configuration = Configuration.load(getClass().getResource("/configuration-test.yml").openStream());
        configuration.setWorkspace(opencgaHome.resolve("sessions").toUri().toString());

        // Copy the conf files
        Files.createDirectories(opencgaHome.resolve("conf"));
//            InputStream inputStream = getClass().getResource("/configuration-test.yml").openStream();
//            Files.copy(inputStream, opencgaHome.resolve("conf").resolve("configuration.yml"), StandardCopyOption.REPLACE_EXISTING);
        configuration.serialize(new FileOutputStream(opencgaHome.resolve("conf").resolve("configuration.yml").toString()));

        InputStream inputStream = getClass().getResource("/storage-configuration.yml").openStream();
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("storage-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        inputStream = getClass().getResource("/configuration-test.yml").openStream();
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        inputStream = getClass().getResource("/analysis.properties").openStream();
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("analysis.properties"), StandardCopyOption.REPLACE_EXISTING);

        // Copy the configuration and example demo files
        Files.createDirectories(opencgaHome.resolve("examples"));
        inputStream = new FileInputStream("../opencga-app/app/misc/examples/20130606_g1k.ped");
        Files.copy(inputStream, opencgaHome.resolve("examples").resolve("20130606_g1k.ped"), StandardCopyOption.REPLACE_EXISTING);

        inputStream = new FileInputStream("../opencga-app/app/misc/examples/1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        Files.copy(inputStream, opencgaHome.resolve("examples")
                .resolve("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), StandardCopyOption.REPLACE_EXISTING);

        catalogManager = new CatalogManager(configuration);

        CatalogDemo.createDemoDatabase(catalogManager, "admin", true);

        restServer = new RestServer(opencgaHome);
        restServer.start();

//        catalogManager = new CatalogManager(configuration);
        clientConfiguration = ClientConfiguration.load(getClass().getResourceAsStream("/client-configuration-test.yml"));
        openCGAClient = new OpenCGAClient("user1", "user1_pass", clientConfiguration);
    }

    @Override
    protected void after() {
        super.after();
        try {
            restServer.stop();
            catalogManager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
