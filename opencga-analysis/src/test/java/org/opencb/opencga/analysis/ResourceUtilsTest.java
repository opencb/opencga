package org.opencb.opencga.analysis;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.analysis.ResourceUtils.ANALYSIS_FOLDER_NAME;
import static org.opencb.opencga.analysis.ResourceUtils.RESOURCES_TXT_FILENAME;

public class ResourceUtilsTest {

    private final static String RESOURCES_URL = "http://resources.opencb.org/task-6766/";
    private static Path opencgaHome;

    @BeforeClass
    public static void before() throws IOException {
        int c = 0;
        do {
            opencgaHome = Paths.get("target/test-data").resolve("junit_opencga_home_" + TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (opencgaHome.toFile().exists());
        Files.createDirectories(opencgaHome);

        // QC
        Path analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/qc")).toAbsolutePath();
        FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/qc/resources.txt");
        Files.copy(inputStream, analysisPath.resolve("resources.txt"), StandardCopyOption.REPLACE_EXISTING);
    }


    @Test
    public void testDownloadQcResources() throws IOException {
        String analysisId = "qc";
        Assume.assumeTrue(areResourcesAvailable(analysisId));

        System.out.println("opencgaHome = " + opencgaHome.toAbsolutePath());
        Path resourcesTxt = opencgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(analysisId).resolve(RESOURCES_TXT_FILENAME);
        List<String> filenames = ResourceUtils.readAllLines(resourcesTxt);
        System.out.println("filenames.size() = " + filenames.size());

        List<File> resourceFiles = ResourceUtils.getResourceFiles(RESOURCES_URL, analysisId, opencgaHome);
        assertEquals(filenames.size(), resourceFiles.size());
    }

    private boolean areResourcesAvailable(String analysisId) {
        List<String> filenames;
        try {
            Path resourcesTxt = opencgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(analysisId).resolve(RESOURCES_TXT_FILENAME);
            filenames = ResourceUtils.readAllLines(resourcesTxt);
        } catch (IOException e) {
            return false;
        }

        for (String filename : filenames) {
            String fileUrl = RESOURCES_URL + analysisId + "/" + filename;
            System.out.println("Checking " + fileUrl + " ...");
            try (BufferedInputStream in = new BufferedInputStream(new URL(fileUrl).openStream());) {
                System.out.println("Ok.");
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }


}