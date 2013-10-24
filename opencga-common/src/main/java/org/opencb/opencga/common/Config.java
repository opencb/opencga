package org.opencb.opencga.common;

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
    private static String opencgaLightHome;
    private static boolean log4jReady = false;

    private static Properties accountProperties = null;
    private static Properties analysisProperties = null;
    private static Properties localServerProperties = null;

    private static long lastPropertyLoad = System.currentTimeMillis();

    public static String getGcsaHome() {
        return opencgaHome;
    }

    public static void setGcsaHome(String gcsaHome) {
        Config.opencgaHome = gcsaHome;

        accountProperties = null;
        analysisProperties = null;
        localServerProperties = null;

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
                logger.error("failed to load account.properties.");
                return null;
            }
        }
        return accountProperties;
    }

    public static Properties getAnalysisProperties() {
        // checkPopertiesStatus();
        if (analysisProperties == null) {
            Path path = Paths.get(opencgaHome, "conf", "analysis.properties");
            analysisProperties = new Properties();
            try {
                analysisProperties.load(Files.newInputStream(path));
            } catch (IOException e) {
                logger.error("failed to load analysis.properties.");
                return null;
            }
        }
        return analysisProperties;
    }

    public static Properties getLocalServerProperties(String basePath) {
        opencgaLightHome = basePath;

//		// First time we create the object, a singleton pattern is applied.
//		if (localServerProperties == null) {
//			loadProperties(localServerProperties, Paths.get(basePath, "conf", "localserver.properties"));
//			return localServerProperties;
//		}
//
//		// next times we check last time loaded
//		checkPopertiesStatus();
//		return localServerProperties;

        if (localServerProperties == null) {
            Path path = Paths.get(basePath, "conf", "localserver.properties");
            localServerProperties = new Properties();
            try {
                localServerProperties.load(Files.newInputStream(path));
            } catch (IOException e) {
                logger.error("failed to load localServer.properties.");
                return null;
            }
        }
        return localServerProperties;
    }

    private static void loadProperties(Properties propertiesToLoad, Path propertiesPath) {
        if (propertiesToLoad != null) {
            propertiesToLoad = new Properties();
        }
        try {
            propertiesToLoad.clear();
            propertiesToLoad.load(Files.newInputStream(propertiesPath));
        } catch (IOException e) {
            logger.error("failed to load: " + propertiesPath.toString());
            e.printStackTrace();
        }
    }

    private static void checkPopertiesStatus() {
        if (System.currentTimeMillis() - lastPropertyLoad > 60000) {
            loadProperties(accountProperties, Paths.get(opencgaHome, "conf", "account.properties"));
            loadProperties(analysisProperties, Paths.get(opencgaHome, "conf", "analysis.properties"));
            loadProperties(localServerProperties, Paths.get(opencgaLightHome, "conf", "localserver.properties"));
            lastPropertyLoad = System.currentTimeMillis();
        }
    }

}
