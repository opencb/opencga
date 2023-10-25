package org.opencb.opencga.app.cli.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

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
    public static final String CLI_USAGE_FILENAME = "cli-usage.yml";

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

    private static CliUsage loadConfiguration() throws IOException {
        // Loading the YAML file from the /conf folder
        String appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));
        String conf = appHome + "/conf";
        File file = new File(conf + File.separator + CLI_USAGE_FILENAME);

        // Mapping the config from the YAML file to the Configuration class
        logger.info("Loading CLI configuration from: " + file.getAbsolutePath());
        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
        return yamlObjectMapper.readValue(file, CliUsage.class);
    }

    /*
     * We keep an instance of cliUsage for the Shell
     */
    public static CliUsage getUsage() {
        if (cliUsage == null) {
            try {
                cliUsage = loadConfiguration();
            } catch (IOException e) {
                logger.error("Loading CLI configuration from: " + CLI_USAGE_FILENAME + " Failed");
            }
        }
        return cliUsage;
    }

    public static void setUsage(CliUsage cliUsage) {
        CliConfiguration.cliUsage = cliUsage;
    }
}
