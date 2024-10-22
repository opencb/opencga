package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.resource.AnalysisResource;
import org.opencb.opencga.core.models.resource.AnalysisResourceList;
import org.opencb.opencga.core.models.resource.ResourceMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.utils.ResourceManager.*;

public class ResourceManagerTest extends AbstractManagerTest {

    Path openCgaHome;
    Path scratchDir;
    Path analysisPath;
    Path analysisResourcePath;
    ResourceMetadata resourceMetadata;
    ResourceManager resourceManager;

    String BASEURL = "http://resources.opencb.org/task-6766/";

    @Before
    public void setUp() throws Exception {
        super.setUp();

        openCgaHome = catalogManagerResource.getOpencgaHome();
        scratchDir = createDir("scratchdir");

        analysisPath = openCgaHome.resolve(ANALYSIS_FOLDER_NAME);
        analysisResourcePath = analysisPath.resolve(RESOURCES_FOLDER_NAME);

        resourceManager = new ResourceManager(openCgaHome, BASEURL);

        resourceMetadata = createResourceMetadata();
        JacksonUtils.getDefaultObjectMapper().writerFor(ResourceMetadata.class).writeValue(scratchDir.resolve(getResourceMetaFilename()).toFile(), resourceMetadata);
    }

    @Test
    public void testConstructor() {
        Assert.assertEquals(GitRepositoryState.getInstance().getBuildVersion().split("-")[0], resourceManager.getVersion());
        Assert.assertTrue(resourceManager.getConfiguration() == null);
        System.out.println("resourceManager = " + resourceManager);
    }


    @Test(expected = ResourceException.class)
    public void testFetchRelatednessResource() throws ResourceException {
        String analysisId = "qc"; //""relatedness";
        String resourceName = "relatedness_thresholds.tsv"; //""variants.prune.in";

        Assert.assertFalse(Files.exists(analysisResourcePath.resolve(analysisId).resolve(resourceName)));

        File file = resourceManager.getResourceFile(analysisId, resourceName);
    }

    @Test
    public void testFetchAllResources() throws IOException, ResourceException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_FOLDER_NAME);
        System.out.println("outDir = " + outDir.toAbsolutePath());
        resourceManager.fetchAllResources(outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());
        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
            for (AnalysisResource resource : list.getResources()) {
                String name = org.apache.commons.lang3.StringUtils.isNotEmpty(resource.getName())
                        ? resource.getName()
                        : Paths.get(resource.getPath()).getFileName().toString();
                Assert.assertTrue(Files.exists(analysisResourcePath.resolve(list.getAnalysisId()).resolve(name)));
            }
        }
    }

    @Test(expected = ResourceException.class)
    public void testFetchAllResourcesNoAdmin() throws ResourceException {
        resourceManager.fetchAllResources(null, catalogManagerResource.getCatalogManager(), normalToken1);
    }

    @Test
    public void testFetchAllResourcesNoOverwrite() throws IOException, ResourceException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_FOLDER_NAME);
        System.out.println("outDir = " + outDir.toAbsolutePath());
        resourceManager.fetchAllResources(outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());
        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
            for (AnalysisResource resource : list.getResources()) {
                String name = org.apache.commons.lang3.StringUtils.isNotEmpty(resource.getName())
                        ? resource.getName()
                        : Paths.get(resource.getPath()).getFileName().toString();
                Assert.assertTrue(Files.exists(analysisResourcePath.resolve(list.getAnalysisId()).resolve(name)));
            }
        }

        resourceManager.fetchAllResources(outDir, catalogManagerResource.getCatalogManager(),
                catalogManagerResource.getAdminToken());
    }

    @Test
    public void testFetchResourcesForAGivenAnalysis() throws IOException, ResourceException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_FOLDER_NAME);
        System.out.println("outDir = " + outDir.toAbsolutePath());
        resourceManager.fetchAllResources(outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());

        String analysisId = "qc";
        AnalysisResourceList analysisResourceList = null;
        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
            if (analysisId.equals(list.getAnalysisId())) {
                analysisResourceList = list;
                break;
            }
        }

        List<File> resourceFiles = resourceManager.getResourceFiles(analysisId);
        Assert.assertEquals(analysisResourceList.getResources().size(), resourceFiles.size());
        for (File resourceFile : resourceFiles) {
            Assert.assertTrue(Files.exists(resourceFile.toPath()));
            Assert.assertTrue(analysisResourceList.getResources().stream().map(r -> StringUtils.isNotEmpty(r.getName()) ? r.getName() : Paths.get(r.getPath()).getFileName().toString()).collect(Collectors.toList()).contains(resourceFile.getName()));
        }
    }

    @Test
    public void testFetchAGivenResource() throws IOException, ResourceException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        Path outDir = createDir("jobdir").resolve(RESOURCES_FOLDER_NAME);
        System.out.println("outDir = " + outDir.toAbsolutePath());
        resourceManager.fetchAllResources(outDir, catalogManagerResource.getCatalogManager(), catalogManagerResource.getAdminToken());

        String analysisId = "liftover";
        String resourceName = "chain.frq";
        AnalysisResourceList analysisResourceList = null;
        for (AnalysisResourceList list : resourceMetadata.getAnalysisResourceLists()) {
            if (analysisId.equals(list.getAnalysisId())) {
                analysisResourceList = list;
                break;
            }
        }

        File resourceFile = resourceManager.getResourceFile(analysisId, resourceName);
        Assert.assertTrue(Files.exists(resourceFile.toPath()));
        Assert.assertTrue(analysisResourceList.getResources().stream().map(r -> StringUtils.isNotEmpty(r.getName()) ? r.getName() : Paths.get(r.getPath()).getFileName().toString()).collect(Collectors.toList()).contains(resourceFile.getName()));
    }

    //-------------------------------------------------------------------------
    // P R I V A T E
    //-------------------------------------------------------------------------

    private ResourceMetadata createResourceMetadata() {
        ResourceMetadata resourceMetadata = new ResourceMetadata();
        resourceMetadata.setVersion(resourceManager.getVersion());

        // Liftover
        AnalysisResourceList analysisResourceList = new AnalysisResourceList();
        analysisResourceList.setAnalysisId("liftover");
        AnalysisResource analysisResource = new AnalysisResource();
        analysisResource.setPath("data/chain.frq");
        analysisResourceList.getResources().add(analysisResource);
        resourceMetadata.getAnalysisResourceLists().add(analysisResourceList);

        // QC
        analysisResourceList = new AnalysisResourceList();
        analysisResourceList.setAnalysisId("qc");
        analysisResource = new AnalysisResource();
        analysisResource.setPath("data/relatedness_thresholds.tsv");
        analysisResourceList.getResources().add(analysisResource);
        analysisResource = new AnalysisResource();
        analysisResource.setPath("data/prune.out");
        analysisResourceList.getResources().add(analysisResource);
        resourceMetadata.getAnalysisResourceLists().add(analysisResourceList);

        return resourceMetadata;
    }

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