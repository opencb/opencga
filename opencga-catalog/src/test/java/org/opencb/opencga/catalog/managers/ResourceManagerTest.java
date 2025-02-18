package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.ToolException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;
import static org.opencb.opencga.catalog.utils.ResourceManager.RESOURCES_DIRNAME;

public class ResourceManagerTest extends AbstractManagerTest {

    private Path openCgaHome;
    private Path scratchDir;
    private Path analysisPath;
    private Path analysisResourcePath;
    private ResourceManager resourceManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        openCgaHome = catalogManagerResource.getOpencgaHome();
        scratchDir = createDir("scratchdir");

        analysisPath = openCgaHome.resolve(ANALYSIS_DIRNAME);
        analysisResourcePath = analysisPath.resolve(RESOURCES_DIRNAME);

        Configuration configuration = Configuration.load(Files.newInputStream(openCgaHome.resolve("conf").resolve("configuration.yml").toFile().toPath()));
        configuration.getAnalysis().getResource().setBasePath(openCgaHome.resolve("analysis/resources").toAbsolutePath());
        configuration.serialize(Files.newOutputStream(openCgaHome.resolve("conf").resolve("configuration.yml").toFile().toPath()));
        resourceManager = new ResourceManager(openCgaHome);
    }

    @Test(expected = ResourceException.class)
    public void testNotFetchedResource() throws ResourceException, ToolException, IOException {
        resourceManager.checkResourcePaths("relatedness");
    }

    //    @Test
//    public void testFetchAllResources() throws IOException, ResourceException {
//        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
//        Path outDir = createDir("jobdir").resolve(RESOURCES_DIRNAME);
//        Files.createDirectories(outDir);
//        System.out.println("outDir = " + outDir.toAbsolutePath());
//        resourceManager.fetchAllResources(version, outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());
//        resourceMetadata = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class).readValue(openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME).resolve(resourceManager.getResourceMetaFilename(version)).toFile());
//
//        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
//            for (AnalysisResource resource : list.getResources()) {
//                Assert.assertTrue(Files.exists(analysisResourcePath.resolve(resource.getLocalRelativePath())));
//            }
//        }
//    }
//
    @Test(expected = ResourceException.class)
    public void testFetchAllResourcesNoAdmin() throws ResourceException, IOException {
        resourceManager.fetchAllResources(null, catalogManagerResource.getCatalogManager(), normalToken1);
    }

    @Test
    public void testFetchResources() throws IOException, ResourceException, ToolException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_DIRNAME);
        Files.createDirectories(outDir);
        System.out.println("outDir = " + outDir.toAbsolutePath());

        resourceManager.fetchResources(Arrays.asList("RELATEDNESS_VARIANTS_PRUNE_IN", "RELATEDNESS_VARIANTS_FRQ"), outDir,
                catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());

        resourceManager.checkResourcePaths("relatedness");
    }

    @Test
    public void testFetchAGivenResource() throws IOException, ResourceException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_DIRNAME);
        Files.createDirectories(outDir);
        System.out.println("outDir = " + outDir.toAbsolutePath());

        resourceManager.fetchResources(Arrays.asList("RELATEDNESS_VARIANTS_PRUNE_IN"), outDir,
                catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());

        resourceManager.checkResourcePath("RELATEDNESS_VARIANTS_PRUNE_IN");
    }

    //-------------------------------------------------------------------------
    // P R I V A T E
    //-------------------------------------------------------------------------

    private Path createDir(String name) throws IOException {
        Path path;
        int c = 0;
        do {
            path = Paths.get("target/test-data").resolve("junit_opencga_" + name + "_"+ TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (path.toFile().exists());

        Files.createDirectories(path);
        return path;
    }
}