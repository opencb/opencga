package org.opencb.opencga.lib.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class Config {

    protected static Logger logger = LoggerFactory.getLogger(Config.class);

    private static String opencgaHome = System.getenv("OPENCGA_HOME");
//    private static String opencgaLightHome;
    private static boolean log4jReady = false;

    private static Properties accountProperties = null;
    private static Properties analysisProperties = null;
    private static Properties storageProperties = null;
    private static Properties catalogProperties = null;

    private static long lastPropertyLoad = System.currentTimeMillis();

    public static String getGcsaHome() {
        return opencgaHome;
    }

    public static void setGcsaHome(String gcsaHome) {
        Config.opencgaHome = gcsaHome;

        accountProperties = null;
        analysisProperties = null;
        storageProperties = null;
        catalogProperties = null;

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

    public static Properties getAccountProperties() {
        // checkPopertiesStatus();
        if (accountProperties == null) {
            Path path = Paths.get(opencgaHome, "conf", "account.properties");
            accountProperties = new Properties();
            try {
                accountProperties.load(Files.newInputStream(path));
            } catch (IOException e) {
                logger.error("Failed to load account.properties: " + e.getMessage());
                return null;
            }
        }
        return accountProperties;
    }

    public static Properties getCatalogProperties() {
        // checkPopertiesStatus();
        if (catalogProperties == null) {
            Path path = Paths.get(opencgaHome, "conf", "catalog.properties");
            catalogProperties = new Properties();
            try {
                catalogProperties.load(Files.newInputStream(path));
            } catch (IOException e) {
                logger.error("Failed to load catalog.properties: " + e.getMessage());
                return null;
            }
        }
        return catalogProperties;
    }

    public static Properties getAnalysisProperties() {
        // checkPopertiesStatus();
        if (analysisProperties == null) {
            Path path = Paths.get(opencgaHome, "conf", "analysis.properties");
            analysisProperties = new Properties();
            try {
                analysisProperties.load(Files.newInputStream(path));
            } catch (IOException e) {
                logger.error("Failed to load analysis.properties: " + e.getMessage());
                return null;
            }
        }
        return analysisProperties;
    }

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
            Path path = Paths.get(basePath, "conf", "storage.properties");
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
            loadProperties(accountProperties, Paths.get(opencgaHome, "conf", "account.properties"));
            loadProperties(analysisProperties, Paths.get(opencgaHome, "conf", "analysis.properties"));
            loadProperties(storageProperties, Paths.get(opencgaHome, "conf", "storage.properties"));
            lastPropertyLoad = System.currentTimeMillis();
        }
    }

}
