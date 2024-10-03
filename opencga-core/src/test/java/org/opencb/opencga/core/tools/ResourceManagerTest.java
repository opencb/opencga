package org.opencb.opencga.core.tools;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.opencb.opencga.core.tools.ResourceManager.*;

public class ResourceManagerTest {

    Path openCgaHome;
    Path analysisPath;
    Path analysisResourcePath;

    @Before
    public void before() throws IOException {
        int c = 0;
        do {
            openCgaHome = Paths.get("target/test-data").resolve("junit_opencga_" + TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (openCgaHome.toFile().exists());
        Files.createDirectories(openCgaHome);

        Path folderConf = Files.createDirectories(openCgaHome.resolve(CONF_FOLDER_NAME));
        BufferedInputStream inputStream = (BufferedInputStream) ResourceManager.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILENAME);
        Files.copy(inputStream, folderConf.resolve(CONFIGURATION_FILENAME), StandardCopyOption.REPLACE_EXISTING);

        analysisPath = Files.createDirectories(openCgaHome.resolve(ANALYSIS_FOLDER_NAME));
        analysisResourcePath = Files.createDirectories(analysisPath.resolve(RESOURCES_FOLDER_NAME));
    }

    @Test
    public void testConstructor() {
        ResourceManager resourceManager = new ResourceManager(openCgaHome);
        Assert.assertEquals(GitRepositoryState.getInstance().getBuildVersion(), resourceManager.getVersion().toString());
        Assert.assertTrue(resourceManager.getConfiguration() == null);
        System.out.println("resourceManager = " + resourceManager);
    }


    @Test
    public void testDonwloadRelatednessResource() throws IOException {
        ResourceManager resourceManager = new ResourceManager(openCgaHome);
        String analysisId = "relatedness";
        String resourceName = "variants.prune.in";

        Assert.assertFalse(Files.exists(analysisResourcePath.resolve(analysisId).resolve(resourceName)));

        File file = resourceManager.getResourceFile(analysisId, resourceName);
        Assert.assertTrue(Files.exists(file.toPath()));
        Assert.assertTrue(Files.exists(analysisResourcePath.resolve(analysisId).resolve(resourceName)));

        File file1 = resourceManager.getResourceFile(analysisId, resourceName);
        Assert.assertTrue(Files.exists(file1.toPath()));
        System.out.println("openCgaHome = " + openCgaHome);
    }
}