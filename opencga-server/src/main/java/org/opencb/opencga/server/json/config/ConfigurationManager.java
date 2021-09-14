package org.opencb.opencga.server.json.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.opencga.server.json.beans.RestApi;

import java.io.File;

public class ConfigurationManager {

    public static Configuration setUp(RestApi restApi) throws Exception {
        // Loading the YAML file from the /resources folder
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("cliConfig.yaml").getFile());
        // Instantiating a new ObjectMapper as a YAMLFactory
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        // Mapping the config from the YAML file to the Configuration class
        Configuration config = om.readValue(file, Configuration.class);
        return config;
    }
}
