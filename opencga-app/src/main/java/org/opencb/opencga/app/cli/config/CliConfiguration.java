package org.opencb.opencga.app.cli.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public class CliConfiguration {

    /**
     * The instance of Configuration that this Class is storing
     */
    private static CliConfiguration instance;

    /**
     * The instance of Usage that stores the "usage" information
     */
    private CliUsage cliUsage;

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
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(cliUsageFileName)) {
            // Mapping the config from the YAML file to the Configuration class
            ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
            return yamlObjectMapper.readValue(is, CliUsage.class);
        }
    }

    /*
     * We keep an instance of cliUsage for the Shell
     */
    public CliUsage getUsage() {
        if (cliUsage == null) {
            try {
                cliUsage = loadConfiguration();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return cliUsage;
    }

    public void setUsage(CliUsage cliUsage) {
        this.cliUsage = cliUsage;
    }

    public String getCliUsageFileName() {
        return cliUsageFileName;
    }

    public void setCliUsageFileName(String cliUsageFileName) {
        this.cliUsageFileName = cliUsageFileName;
    }
}
