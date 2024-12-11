package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
//    @Test(expected = ResourceException.class)
//    public void testFetchAllResourcesNoAdmin() throws ResourceException {
//        resourceManager.fetchAllResources(version, null, catalogManagerResource.getCatalogManager(), normalToken1);
//    }
//
//    @Test
//    public void testFetchResourcesForAGivenAnalysis() throws IOException, ResourceException {
//        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
//        Path outDir = createDir("jobdir").resolve(RESOURCES_DIRNAME);
//        Files.createDirectories(outDir);
//        System.out.println("outDir = " + outDir.toAbsolutePath());
//        resourceManager.fetchAllResources(version, outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());
//        resourceMetadata = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class).readValue(openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME).resolve(resourceManager.getResourceMetaFilename(version)).toFile());
//
//        String analysisId = "qc";
//        AnalysisResourceList analysisResourceList = null;
//        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
//            if (analysisId.equals(list.getAnalysisId())) {
//                analysisResourceList = list;
//                break;
//            }
//        }
//
//        List<File> resourceFiles = resourceManager.getResourceFiles(analysisId, version);
//        Assert.assertEquals(analysisResourceList.getResources().size(), resourceFiles.size());
//        for (File resourceFile : resourceFiles) {
//            Assert.assertTrue(Files.exists(resourceFile.toPath()));
//            Assert.assertTrue(analysisResourceList.getResources().stream().map(r -> Paths.get(r.getLocalRelativePath()).getFileName().toString()).collect(Collectors.toList()).contains(resourceFile.getName()));
//        }
//    }
//
//    @Test
//    public void testFetchAGivenResource() throws IOException, ResourceException {
//        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
//        Path outDir = createDir("jobdir").resolve(RESOURCES_DIRNAME);
//        Files.createDirectories(outDir);
//        System.out.println("outDir = " + outDir.toAbsolutePath());
//        resourceManager.fetchAllResources(version, outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());
//        resourceMetadata = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class).readValue(openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME).resolve(resourceManager.getResourceMetaFilename(version)).toFile());
//
//        String analysisId = "liftover";
//        String resourceName = "chain.frq";
//        AnalysisResourceList analysisResourceList = null;
//        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
//            if (analysisId.equals(list.getAnalysisId())) {
//                analysisResourceList = list;
//                break;
//            }
//        }
//
//        File resourceFile = resourceManager.getResourceFile(analysisId, resourceName, version);
//        Assert.assertTrue(Files.exists(resourceFile.toPath()));
//        Assert.assertTrue(analysisResourceList.getResources().stream().map(r -> Paths.get(r.getLocalRelativePath()).getFileName().toString()).collect(Collectors.toList()).contains(resourceFile.getName()));
//    }

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