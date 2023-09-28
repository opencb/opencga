package org.opencb.opencga.app.cli.conf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class Configuration {
    /**
     * The instance of Usage that stores the "usage" information
     */
    private static Usage usage;

    /**
     * LOGGER is an instance of the Logger class so that we can do proper
     * logging
     */
    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    /**
     * The instance of Configuration that this Class is storing
     */
    private static Configuration instance = null;

    /**
     * FILENAME is the file location of the configuration yml file
     */
    public static final String FILENAME = "usage.yml";

    public static Configuration getInstance() {
        if (Configuration.instance == null) {
            Configuration.instance = new Configuration();
        }

        return Configuration.instance;
    }

    private static Usage loadConfiguration() throws IOException {
        // Loading the YAML file from the /conf folder
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));
        String conf = appHome + "/conf";
        File file = new File(conf + File.separator + FILENAME);

        // Mapping the config from the YAML file to the Configuration class
        logger.info("Loading CLI configuration from: " + file.getAbsolutePath());
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        return om.readValue(file, Usage.class);
    }

    public static Usage getUsage() {
        if (usage == null) {
            try {
                usage = loadConfiguration();
            } catch (IOException e) {
                logger.error("Loading CLI configuration from: " + FILENAME + " Failed");

            }
        }
        return usage;
    }

    public static void setUsage(Usage usage) {
        Configuration.usage = usage;
    }
}
