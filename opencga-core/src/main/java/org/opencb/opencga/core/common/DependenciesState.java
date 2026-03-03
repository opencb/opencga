package org.opencb.opencga.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class DependenciesState {

    private static DependenciesState instance;
    private static final String DEPENDENCIES_RESOURCE_NAME = "org/opencb/opencga/core/dependencies.properties";

    private Properties properties;

    private DependenciesState(Properties properties) {
        this.properties = properties;
    }

    public static DependenciesState getInstance() {
        if (instance == null) {
            Logger logger = LoggerFactory.getLogger(DependenciesState.class);
            Properties properties = new Properties();
            try (InputStream stream = DependenciesState.class.getClassLoader().getResourceAsStream(DEPENDENCIES_RESOURCE_NAME)) {
                if (stream != null) {
                    properties.load(stream);
                }
            } catch (Exception e) {
                logger.warn("Could not load dependencies properties from resource: {}", DEPENDENCIES_RESOURCE_NAME, e);
            }
            instance = new DependenciesState(properties);
        }
        return instance;
    }

    public String getDependencyVersion(String dependencyName) {
        return properties.getProperty(dependencyName);
    }

    public String getOpenCGAVersion() {
        return properties.getProperty("opencga.version");
    }

    public String getBiodataVersion() {
        return properties.getProperty("biodata.version");
    }

    public String getCellBaseVersion() {
        return properties.getProperty("cellbase.version");
    }

    public String getJavaCommonsLibsVersion() {
        return properties.getProperty("java-common-libs.version");
    }

}
