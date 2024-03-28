package com.zettagenomics.opencga.enterprise.core.configuration;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.core.util.FileUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;


public class EnterpriseConfigurationTest {
/*
    @Test
    public void testLoadingConfigString() throws IOException {
        InputStream is = EnterpriseConfigurationTest.class.getClassLoader().getResourceAsStream("enterprise-configuration.yml");
        EnterpriseConfiguration config = EnterpriseConfiguration.load(is);
        System.out.println(config);
    }

    @Test
    public void testLoadingConfigFile() throws IOException {
        URL url = EnterpriseConfigurationTest.class.getClassLoader().getResource("enterprise-configuration.yml");
        Path resourcePath = Paths.get(url.getPath());
        Path opencgaHome = resourcePath.getParent().resolve(RandomStringUtils.randomAlphabetic(10));
        Path configPath = opencgaHome.resolve("conf");
        Path enterprisePath = configPath.resolve(resourcePath.getFileName());
        FileUtils.makeParentDirs(enterprisePath.toFile());
        Files.copy(resourcePath, enterprisePath);
        assertEquals(Boolean.TRUE, enterprisePath.toFile().exists());
        System.out.println(enterprisePath.toAbsolutePath());
        EnterpriseConfiguration config = EnterpriseConfiguration.load(opencgaHome);
        System.out.println(config);
    }*/
}