/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Deprecated
public class Config {

    public static final String ACCOUNT_PROPERTIES = "account.properties";
    public static final String CATALOG_PROPERTIES = "catalog.properties";
    public static final String ANALYSIS_PROPERTIES = "analysis.properties";
    public static final String STORAGE_PROPERTIES = "storage.properties";
    protected static Logger logger = LoggerFactory.getLogger(Config.class);

    private static String opencgaHome = null;
//    private static String opencgaLightHome;
    private static boolean log4jReady = false;

    private static Map<String, Properties> propertiesMap = new HashMap<>();

    private static Properties storageProperties = null;

    private static long lastPropertyLoad = System.currentTimeMillis();

    public static String getOpenCGAHome() {
        return opencgaHome;
    }

    public static String setAutoOpenCGAHome() {
        // Finds the installation directory (opencgaHome).
        // Searches first in System Property "app.home" set by the shell script.
        // If not found, then in the environment variable "OPENCGA_HOME".
        // If none is found, it supposes "debug-mode" and the opencgaHome is in .../opencga/opencga-app/build/
        String propertyAppHome = System.getProperty("app.home");
        logger.debug("propertyAppHome = {}", propertyAppHome);
        if (propertyAppHome != null) {
            opencgaHome = propertyAppHome;
        } else {
            String envAppHome = System.getenv("OPENCGA_HOME");
            if (envAppHome != null) {
                opencgaHome = envAppHome;
            } else {
                opencgaHome = Paths.get(".", "build").toString(); //If it has not been run from the shell script (debug)
            }
        }
        Config.setOpenCGAHome(opencgaHome);
        return opencgaHome;
    }

    public static void setOpenCGAHome(String opencgaHome) {
        Config.opencgaHome = opencgaHome;
        propertiesMap.clear();
    }

    @Deprecated
    public static String getGcsaHome() {
        return opencgaHome;
    }

    @Deprecated
    public static void setGcsaHome(String gcsaHome) {
        Config.opencgaHome = gcsaHome;

        storageProperties = null;

        log4jReady = false;
//        LogManager.resetConfiguration();
//        configureLog4j();
    }

//    public static void configureLog4j() {
//        if (!log4jReady) {
//            Path path = Paths.get(opencgaHome, "conf", "log4j.properties");
//            try {
//                PropertyConfigurator.configure(Files.newInputStream(path));
//            } catch (IOException e) {
//                BasicConfigurator.configure();
//                logger.warn("failed to load log4j.properties, BasicConfigurator will be used.");
//            }
//            log4jReady = true;
//        }
//    }


    public static Properties getProperties(String fileName) {
        return getProperties(fileName, null);
    }

    public static Properties getProperties(String fileName, Properties defaultProperties) {
        if(!propertiesMap.containsKey(fileName)) {
            Path path = Paths.get(opencgaHome, "conf", fileName);
            Properties properties = new Properties(defaultProperties);
            try {
                properties.load(Files.newInputStream(path));
            } catch (IOException e) {
                logger.error("Failed to load " + fileName + ": " + e.getMessage());
                return defaultProperties;
            }
            propertiesMap.put(fileName, properties);
        }
        return propertiesMap.get(fileName);
    }

    @Deprecated
    public static Properties getAccountProperties() {
        return getProperties(ACCOUNT_PROPERTIES);
    }

    public static Properties getCatalogProperties() {
        return getProperties(CATALOG_PROPERTIES);
    }

    public static Properties getAnalysisProperties() {
        return getProperties(ANALYSIS_PROPERTIES);
    }

    public static Properties getStorageProperties() {
        return getProperties(STORAGE_PROPERTIES);
    }
    @Deprecated
    public static Properties getStorageProperties(String basePath) {
//        opencgaLightHome = basePath;

//		// First time we create the object, a singleton pattern is applied.
//		if (storageProperties == null) {
//			loadProperties(storageProperties, Paths.get(basePath, "conf", "localserver.properties"));
//			return storageProperties;
//		}
//
//		// next times we check last time loaded
//		checkPopertiesStatus();
//		return storageProperties;

        if (storageProperties == null) {
            Path path = Paths.get(basePath, "conf", STORAGE_PROPERTIES);
            storageProperties = new Properties();
            try {
                storageProperties.load(Files.newInputStream(path));
            } catch (IOException e) {
                logger.error("Failed to load storage.properties: " + e.getMessage());
                return null;
            }
        }
        return storageProperties;
    }

    private static void loadProperties(Properties propertiesToLoad, Path propertiesPath) {
        if (propertiesToLoad != null) {
            propertiesToLoad = new Properties();
        }
        try {
            propertiesToLoad.clear();
            propertiesToLoad.load(Files.newInputStream(propertiesPath));
        } catch (IOException e) {
            logger.error("Failed to load: " + propertiesPath.toString());
            e.printStackTrace();
        }
    }


    private static void checkPopertiesStatus() {
        if (System.currentTimeMillis() - lastPropertyLoad > 60000) {
//            loadProperties(accountProperties, Paths.get(opencgaHome, "conf", "account.properties"));
//            loadProperties(analysisProperties, Paths.get(opencgaHome, "conf", "analysis.properties"));
            loadProperties(storageProperties, Paths.get(opencgaHome, "conf", "storage.properties"));
            lastPropertyLoad = System.currentTimeMillis();
        }
    }

}
