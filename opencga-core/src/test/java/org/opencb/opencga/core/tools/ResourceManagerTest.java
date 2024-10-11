package org.opencb.opencga.core.tools;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.resource.AnalysisResource;
import org.opencb.opencga.core.models.resource.ResourceMetadata;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.opencb.opencga.core.tools.ResourceManager.*;

public class ResourceManagerTest {

    Path openCgaHome;
    Path scratchDir;
    Path analysisPath;
    Path analysisResourcePath;
    ResourceMetadata resourceMetadata;
    ResourceManager resourceManager;

    String BASEURL = "http://resources.opencb.org/task-6766/";

    @Before
    public void before() throws IOException {
        openCgaHome = createDir();
        scratchDir = createDir();

        Path folderConf = Files.createDirectories(openCgaHome.resolve(CONF_FOLDER_NAME));
        BufferedInputStream inputStream = (BufferedInputStream) ResourceManager.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILENAME);
        Files.copy(inputStream, folderConf.resolve(CONFIGURATION_FILENAME), StandardCopyOption.REPLACE_EXISTING);

        analysisPath = Files.createDirectories(openCgaHome.resolve(ANALYSIS_FOLDER_NAME));
        analysisResourcePath = Files.createDirectories(analysisPath.resolve(RESOURCES_FOLDER_NAME));

        resourceManager = new ResourceManager(openCgaHome, BASEURL);

        resourceMetadata = createResourceMetadata();
    }

    @Test
    public void testConstructor() {
        Assert.assertEquals(GitRepositoryState.getInstance().getBuildVersion().split("-")[0], resourceManager.getVersion());
        Assert.assertTrue(resourceManager.getConfiguration() == null);
        System.out.println("resourceManager = " + resourceManager);
    }


    @Test
    public void testDonwloadRelatednessResource() throws IOException, NoSuchAlgorithmException {
        String analysisId = "qc"; //""relatedness";
        String resourceName = "relatedness_thresholds.tsv"; //""variants.prune.in";

        Assert.assertFalse(Files.exists(analysisResourcePath.resolve(analysisId).resolve(resourceName)));

        File file = resourceManager.getResourceFile(analysisId, resourceName);
        Assert.assertTrue(Files.exists(file.toPath()));
        Assert.assertTrue(Files.exists(analysisResourcePath.resolve(analysisId).resolve(resourceName)));

        File file1 = resourceManager.getResourceFile(analysisId, resourceName);
        Assert.assertTrue(Files.exists(file1.toPath()));
        System.out.println("openCgaHome = " + openCgaHome);
    }

    @Test
    public void testDownloadAllResources() throws IOException, NoSuchAlgorithmException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        resourceManager.downloadAllResources(true);
        for (AnalysisResource analysisResource : resourceMetadata.getAnalysisResources()) {
            for (String resource : analysisResource.getResources()) {
                Assert.assertTrue(Files.exists(analysisResourcePath.resolve(analysisResource.getId()).resolve(resource)));
            }
        }
    }

    @Test
    public void testDownloadResourcesForAGivenAnalysis() throws IOException, NoSuchAlgorithmException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        resourceManager.downloadAllResources(true);

        String analysisId = "qc";
        AnalysisResource analysisResource = null;
        for (AnalysisResource ar : resourceMetadata.getAnalysisResources()) {
            if (analysisId.equals(ar.getId())) {
                analysisResource = ar;
                break;
            }
        }

        List<File> resourceFiles = resourceManager.getResourceFiles(analysisId);
        Assert.assertEquals(analysisResource.getResources().size(), resourceFiles.size());
        for (File resourceFile : resourceFiles) {
            Assert.assertTrue(Files.exists(resourceFile.toPath()));
            Assert.assertTrue(analysisResource.getResources().contains(resourceFile.getName()));
        }
    }

    @Test
    public void testDownloadAGivenResource() throws IOException, NoSuchAlgorithmException {
        System.out.println("analysisResourcePath = " + analysisResourcePath.toAbsolutePath());
        resourceManager.downloadAllResources(true);

        String analysisId = "liftover";
        String resourceName = "chain.frq";
        AnalysisResource analysisResource = null;
        for (AnalysisResource ar : resourceMetadata.getAnalysisResources()) {
            if (analysisId.equals(ar.getId())) {
                analysisResource = ar;
                break;
            }
        }

        File resourceFile = resourceManager.getResourceFile(analysisId, resourceName);
        Assert.assertTrue(Files.exists(resourceFile.toPath()));
        Assert.assertTrue(analysisResource.getResources().contains(resourceFile.getName()));
    }

    //-------------------------------------------------------------------------
    // P R I V A T E
    //-------------------------------------------------------------------------

    private ResourceMetadata createResourceMetadata() {
        ResourceMetadata resourceMetadata = new ResourceMetadata();
        resourceMetadata.setVersion(resourceManager.getVersion());

        AnalysisResource analysisResource = new AnalysisResource();
        analysisResource.setId("liftover");
        resourceMetadata.getAnalysisResources().add(analysisResource);
        analysisResource.getResources().add("chain.frq");

        analysisResource = new AnalysisResource();
        analysisResource.setId("qc");
        analysisResource.getResources().add("relatedness_thresholds.tsv");
        analysisResource.getResources().add("prune.out");
        resourceMetadata.getAnalysisResources().add(analysisResource);

        return resourceMetadata;
    }

    private Path createDir() throws IOException {
        Path path;
        int c = 0;
        do {
            path = Paths.get("target/test-data").resolve("junit_opencga_" + TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (path.toFile().exists());

        Files.createDirectories(path);
        return path;
    }
}