package com.zettagenomics.opencga.enterprise.core.configuration;

import org.junit.Test;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class EnterpriseConfigurationTest {

    @Test
    public void testLoadingConfigString() throws IOException {
        InputStream is = EnterpriseConfigurationTest.class.getClassLoader().getResourceAsStream("enterprise-configuration.yml");
        EnterpriseConfiguration config = EnterpriseConfiguration.load(is);
        System.out.println(config);
    }

    @Test
    public void testLoadingConfigFile() throws IOException {
        Path opencgaHome;
        int c = 0;
        do {
            opencgaHome = Paths.get("target/test-data").resolve("junit_opencga_home_" + TimeUtils.getTimeMillis() + (c > 0 ? "_" + c : ""));
            c++;
        } while (opencgaHome.toFile().exists());
        Files.createDirectories(opencgaHome);
        System.out.println("OpenCGA home = " + opencgaHome.toAbsolutePath());
        Path configPath = opencgaHome.resolve("conf");

        // Create a File object for the destination file
        InputStream is = EnterpriseConfigurationTest.class.getClassLoader().getResourceAsStream("enterprise-configuration.yml");

        // Create the destination directory if it doesn't exist
        if (!Files.exists(configPath)) {
            Files.createDirectories(configPath);
        }

        // Create the destination file path
        Path destinationFilePath = configPath.resolve("enterprise-configuration.yml");

        // Copy the content of the resource file to the destination file
        try (InputStreamReader inputStreamReader = new InputStreamReader(is);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
             BufferedWriter bufferedWriter = Files.newBufferedWriter(destinationFilePath)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
        }

        EnterpriseConfiguration config = EnterpriseConfiguration.load(opencgaHome);
        System.out.println(config);
    }
}