package org.opencb.opencga.client.rest;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.opencb.opencga.analysis.demo.AnalysisDemo;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.utils.CatalogDemo;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.server.rest.RestServer;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Created by pfurio on 08/06/16.
 */
public class SampleClientTest {

    private OpenCGAClient openCGAClient;
    private SampleClient sampleClient;
    private Path opencgaHome;
    private CatalogManager catalogManager;
    private Configuration configuration;
    private ClientConfiguration clientConfiguration;
    private CatalogConfiguration catalogConfiguration;
    private StorageConfiguration storageConfiguration;

    private static RestServer restServer;

    public SampleClientTest() throws Exception {
        try {
            opencgaHome = Paths.get("target/test-data").resolve("junit_opencga_home_" + RandomStringUtils.randomAlphabetic(10));
            Files.createDirectories(opencgaHome);
            configuration = Configuration.load(getClass().getResource("/configuration-test.yml").openStream());
            storageConfiguration = StorageConfiguration.load(getClass().getResource("/storage-configuration.yml").openStream());
            catalogConfiguration = CatalogConfiguration.load(getClass().getResource("/catalog-configuration-test.yml").openStream());
            catalogConfiguration.setDataDir(opencgaHome.resolve("sessions").toUri().toString());
            catalogConfiguration.setTempJobsDir(opencgaHome.resolve("jobs").toUri().toString());
            catalogConfiguration.getDatabase().setDatabase("opencga_catalog_demo");

            // Copy the conf files
            Files.createDirectories(opencgaHome.resolve("conf"));
//            InputStream inputStream = getClass().getResource("/catalog-configuration-test.yml").openStream();
//            Files.copy(inputStream, opencgaHome.resolve("conf").resolve("catalog-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);
            catalogConfiguration.serialize(
                    new FileOutputStream(opencgaHome.resolve("conf").resolve("catalog-configuration.yml").toString()));

            InputStream inputStream = getClass().getResource("/storage-configuration.yml").openStream();
            Files.copy(inputStream, opencgaHome.resolve("conf").resolve("storage-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

            inputStream = getClass().getResource("/configuration-test.yml").openStream();
            Files.copy(inputStream, opencgaHome.resolve("conf").resolve("configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

            inputStream = getClass().getResource("/analysis.properties").openStream();
            Files.copy(inputStream, opencgaHome.resolve("conf").resolve("analysis.properties"), StandardCopyOption.REPLACE_EXISTING);

//            // Copy the bin files
//            Files.createDirectories(opencgaHome.resolve("bin"));
//            inputStream = new FileInputStream("../opencga-app/target/appassembler/bin/opencga-analysis.sh");
//            Files.copy(inputStream, opencgaHome.resolve("bin").resolve("opencga-analysis.sh"), StandardCopyOption.COPY_ATTRIBUTES);
//
//            inputStream = new FileInputStream("../opencga-app/target/appassembler/bin/opencga.sh");
//            Files.copy(inputStream, opencgaHome.resolve("bin").resolve("opencga.sh"), StandardCopyOption.COPY_ATTRIBUTES);

            // Copy the configuration and example demo files
            Files.createDirectories(opencgaHome.resolve("examples"));
            inputStream = new FileInputStream("../opencga-app/app/examples/20130606_g1k.ped");
            Files.copy(inputStream, opencgaHome.resolve("examples").resolve("20130606_g1k.ped"), StandardCopyOption.REPLACE_EXISTING);

            inputStream = new FileInputStream("../opencga-app/app/examples/1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
            Files.copy(inputStream, opencgaHome.resolve("examples")
                    .resolve("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), StandardCopyOption.REPLACE_EXISTING);

            CatalogDemo.createDemoDatabase(catalogConfiguration, true);

            restServer = new RestServer(opencgaHome.resolve("conf"));
            restServer.start();

            catalogManager = new CatalogManager(catalogConfiguration);
            clientConfiguration = ClientConfiguration.load(getClass().getResourceAsStream("/client-configuration-test.yml"));
            openCGAClient = new OpenCGAClient("user1", "user1_pass", clientConfiguration);

//            AnalysisDemo.insertPedigreeFile(catalogManager, 6L, opencgaHome.resolve("examples/20130606_g1k.ped"),
//                    openCGAClient.getSessionId());
//            AnalysisDemo.insertVariantFile(catalogManager, 6L,
//                    opencgaHome.resolve("examples/1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
//                    openCGAClient.getSessionId());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    static public void shutdownServer() throws Exception {
        restServer.stop();
    }

}
