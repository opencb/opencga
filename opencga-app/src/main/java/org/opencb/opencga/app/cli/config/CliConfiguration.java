package org.opencb.opencga.app.cli.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class CliConfiguration {

    /**
     * The instance of Configuration that this Class is storing
     */
    private static CliConfiguration instance;

    /**
     * The instance of Usage that stores the "usage" information
     */
    private static CliUsage cliUsage;

    /**
     * FILENAME is the file location of the configuration yml file
     */
    private String cliUsageFileName = "cli-usage.yml";

    /**
     * LOGGER is an instance of the Logger class so that we can do proper
     * logging
     */
    private static final Logger logger = LoggerFactory.getLogger(CliConfiguration.class);

    public static CliConfiguration getInstance() {
        if (CliConfiguration.instance == null) {
            CliConfiguration.instance = new CliConfiguration();
        }
        return CliConfiguration.instance;
    }

    private CliUsage loadConfiguration() throws IOException {
        // Loading the YAML file from the /conf folder
        File file = new File(FileUtils.getTempDirectory()+File.separator+cliUsageFileName);

        FileUtils.copyURLToFile(getClass().getClassLoader().getResource(cliUsageFileName),file);

        // Mapping the config from the YAML file to the Configuration class
        logger.info("Loading CLI configuration from: " + file.getAbsolutePath());
        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
        return yamlObjectMapper.readValue(file, CliUsage.class);
    }

    /*
     * We keep an instance of cliUsage for the Shell
     */
    public CliUsage getUsage() {
        if (cliUsage == null) {
            try {
                cliUsage = loadConfiguration();
            } catch (IOException e) {
                logger.error("Loading CLI configuration from: " + cliUsageFileName + " Failed");
                System.err.println("Loading CLI configuration from: " + cliUsageFileName + " Failed");
            }
        }
        return cliUsage;
    }

    public void setUsage(CliUsage cliUsage) {
        CliConfiguration.cliUsage = cliUsage;
    }

    public String getCliUsageFileName() {
        return cliUsageFileName;
    }

    public void setCliUsageFileName(String cliUsageFileName) {
        this.cliUsageFileName = cliUsageFileName;
    }
}
