/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.app.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.*;
import org.opencb.opencga.app.cli.main.UserConfigFile;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 19/04/16.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    protected String logFile;
    protected boolean verbose;
    protected String configFile;
    protected String storageConfigFile;

    protected String appHome;
    protected String sessionId;

    protected CatalogConfiguration catalogConfiguration;
    protected StorageConfiguration storageConfiguration;
    protected ClientConfiguration clientConfiguration;

    protected Logger logger;

    public CommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        init(options);
    }

    protected void init(GeneralCliOptions.CommonCommandOptions options) {
        init(options.logLevel, options.verbose, options.configFile);
    }

    protected void init(String logLevel, boolean verbose, String configFile) {
        this.logLevel = logLevel;
        this.verbose = verbose;
        this.configFile = configFile;

        /**
         * System property 'app.home' is automatically set up in opencga-storage.sh. If by any reason
         * this is 'null' then OPENCGA_HOME environment variable is used instead.
         */
        this.appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));
        Config.setOpenCGAHome(appHome);

        if (verbose) {
            logLevel = "debug";
        }

        if (logLevel != null && !logLevel.isEmpty()) {
            // We must call to this method
            configureDefaultLog(logLevel);
        }

        try {
            UserConfigFile userConfigFile = loadUserFile();
            sessionId = userConfigFile.getSessionId();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public abstract void execute() throws Exception;

    public String getLogLevel() {
        return logLevel;
    }

    public void configureDefaultLog(String logLevel) {
        // This small hack allow to configure the appropriate Logger level from the command line, this is done
        // by setting the DEFAULT_LOG_LEVEL_KEY before the logger object is created.
//        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel);
        org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();
//        rootLogger.setLevel(Level.toLevel(logLevel));

        ConsoleAppender stderr = (ConsoleAppender) rootLogger.getAppender("stderr");
        stderr.setThreshold(Level.toLevel(logLevel));

        //Disable MongoDB useless logging
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.cluster").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.connection").setLevel(Level.WARN);

        logger = LoggerFactory.getLogger(this.getClass().toString());
        this.logLevel = logLevel;
    }


    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }


    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public Logger getLogger() {
        return logger;
    }


    public boolean loadConfigurations() {
        try {
            loadCatalogConfiguration();
        } catch (IOException ex) {
            if (getLogger() == null) {
                ex.printStackTrace();
            } else {
                getLogger().error("Error reading OpenCGA Catalog configuration: " + ex.getMessage());
            }
            return false;
        }
        try {
            loadStorageConfiguration();
            loadClientConfiguration();
        } catch (IOException ex) {
            if (getLogger() == null) {
                ex.printStackTrace();
            } else {
                getLogger().error("Error reading OpenCGA Storage configuration: " + ex.getMessage());
            }
            return false;
        }
        return true;
    }

    /**
     * This method attempts to first data configuration from CLI parameter, if not present then uses
     * the configuration from installation directory, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadCatalogConfiguration() throws IOException {
        String loadedConfigurationFile;
        if (this.configFile != null) {
            loadedConfigurationFile = this.configFile;
            this.catalogConfiguration = CatalogConfiguration.load(new FileInputStream(new File(this.configFile)));
        } else {
            // We load configuration file either from app home folder or from the JAR
            Path path = Paths.get(appHome + "/conf/catalog-configuration.yml");
            if (appHome != null && Files.exists(path)) {
                loadedConfigurationFile = path.toString();
                this.catalogConfiguration = CatalogConfiguration.load(new FileInputStream(path.toFile()));
            } else {
                loadedConfigurationFile = CatalogConfiguration.class.getClassLoader().getResourceAsStream("catalog-configuration.yml")
                        .toString();
                this.catalogConfiguration = CatalogConfiguration
                        .load(CatalogConfiguration.class.getClassLoader().getResourceAsStream("catalog-configuration.yml"));
            }
        }

        // logLevel parameter has preference in CLI over configuration file
        if (this.logLevel == null || this.logLevel.isEmpty()) {
            this.logLevel = this.catalogConfiguration.getLogLevel();
            configureDefaultLog(this.logLevel);
        } else {
            if (!this.logLevel.equalsIgnoreCase(this.catalogConfiguration.getLogLevel())) {
                this.catalogConfiguration.setLogLevel(this.logLevel);
                configureDefaultLog(this.logLevel);
            }
        }

        // logFile parameter has preference in CLI over configuration file, we first set the logFile passed
        if (this.logFile != null && !this.logFile.isEmpty()) {
            this.catalogConfiguration.setLogFile(logFile);
        }

        // If user has set up a logFile we redirect logs to it
        if (this.catalogConfiguration.getLogFile() != null && !this.catalogConfiguration.getLogFile().isEmpty()) {
            org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();

            // If a log file is used then console log is removed
            rootLogger.removeAppender("stderr");

            // Creating a RollingFileAppender to output the log
            RollingFileAppender rollingFileAppender = new RollingFileAppender(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - "
                    + "%m%n"), this.catalogConfiguration.getLogFile(), true);
            rollingFileAppender.setThreshold(Level.toLevel(catalogConfiguration.getLogLevel()));
            rootLogger.addAppender(rollingFileAppender);
        }

        logger.debug("Loading configuration from '{}'", loadedConfigurationFile);
    }


    /**
     * This method attempts to first data configuration from CLI parameter, if not present then uses
     * the configuration from installation directory, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadStorageConfiguration() throws IOException {
        String loadedConfigurationFile;
        if (this.storageConfigFile != null) {
            loadedConfigurationFile = this.storageConfigFile;
            this.storageConfiguration = StorageConfiguration.load(new FileInputStream(new File(this.storageConfigFile)));
        } else {
            // We load configuration file either from app home folder or from the JAR
            Path path = Paths.get(appHome + "/conf/storage-configuration.yml");
            if (appHome != null && Files.exists(path)) {
                loadedConfigurationFile = path.toString();
                this.storageConfiguration = StorageConfiguration.load(new FileInputStream(path.toFile()));
            } else {
                loadedConfigurationFile = StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml")
                        .toString();
                this.storageConfiguration = StorageConfiguration
                        .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
            }
        }
        logger.debug("Loading configuration from '{}'", loadedConfigurationFile);
    }

    /**
     * This method attempts to first data configuration from CLI parameter, if not present then uses
     * the configuration from installation directory, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadClientConfiguration() throws IOException {
        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(appHome + "/conf/client-configuration.yml");
        if (appHome != null && Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.clientConfiguration = ClientConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading configuration from JAR file");
            this.clientConfiguration = ClientConfiguration
                    .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("client-configuration.yml"));
        }
    }


    protected void saveUserFile(String user, String session) throws IOException {
        java.io.File file = Paths.get(System.getProperty("user.home"), ".opencga", "session.json").toFile();
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(file, new UserConfigFile(user, session));
    }

    private static UserConfigFile loadUserFile() throws IOException {
        java.io.File file = Paths.get(System.getProperty("user.home"), ".opencga", "session.yml").toFile();
        if (file.exists()) {
            return new ObjectMapper(new YAMLFactory()).readValue(file, UserConfigFile.class);
        } else {
            return new UserConfigFile();
        }
    }

//    protected boolean runCommandLineProcess(File workingDirectory, String binPath, List<String> args, String logFilePath)
//            throws IOException, InterruptedException {
//        ProcessBuilder builder = getProcessBuilder(workingDirectory, binPath, args, logFilePath);
//
//        logger.debug("Executing command: " + StringUtils.join(builder.command(), " "));
//        Process process = builder.start();
//        process.waitFor();
//
//        // Check process output
//        boolean executedWithoutErrors = true;
//        int genomeInfoExitValue = process.exitValue();
//        if (genomeInfoExitValue != 0) {
//            logger.warn("Error executing {}, error code: {}. More info in log file: {}", binPath, genomeInfoExitValue, logFilePath);
//            executedWithoutErrors = false;
//        }
//        return executedWithoutErrors;
//    }
//
//    private ProcessBuilder getProcessBuilder(File workingDirectory, String binPath, List<String> args, String logFilePath) {
//        List<String> commandArgs = new ArrayList<>();
//        commandArgs.add(binPath);
//        commandArgs.addAll(args);
//        ProcessBuilder builder = new ProcessBuilder(commandArgs);
//
//        // working directoy and error and output log outputs
//        if (workingDirectory != null) {
//            builder.directory(workingDirectory);
//        }
//        builder.redirectErrorStream(true);
//        if (logFilePath != null) {
//            builder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logFilePath)));
//        }
//
//        return builder;
//    }

    public CatalogConfiguration getCatalogConfiguration() {
        return catalogConfiguration;
    }

    public void setCatalogConfiguration(CatalogConfiguration catalogConfiguration) {
        this.catalogConfiguration = catalogConfiguration;
    }

}
