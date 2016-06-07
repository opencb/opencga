package org.opencb.opencga.app.demo;

import org.junit.Rule;
import org.junit.Test;
import org.opencb.opencga.analysis.storage.OpenCGATestExternalResource;

/**
 * Created by pfurio on 20/05/16.
 */
public class CatalogManagerDemoTest {

    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    @Test
    public void testCreateDemoDatabase() throws Exception {
//        opencgaHome = Paths.get(System.getProperty("java.io.tmpdir")).resolve("junit_opencga_home");
//        catalogConfiguration = CatalogConfiguration.load(CatalogManagerDemo.class.getResource("/catalog-configuration-test.yml")
//                .openStream());
//        catalogConfiguration.setDataDir(opencgaHome.resolve("sessions").toUri().toString());
//        catalogConfiguration.setTempJobsDir(opencgaHome.resolve("jobs").toUri().toString());
//
//        catalogConfiguration.getDatabase().setDatabase("opencga_catalog_demo");
//        catalogManager = new CatalogManager(catalogConfiguration);
        CatalogManagerDemo.createDemoDatabase(opencga.getCatalogManager(), true);
    }
}