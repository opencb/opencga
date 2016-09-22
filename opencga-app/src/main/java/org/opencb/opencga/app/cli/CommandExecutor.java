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
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.*;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.app.cli.analysis.AnalysisCliOptionsParser;
import org.opencb.opencga.app.cli.main.SessionFile;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;

/**
 * Created by imedina on 19/04/16.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    protected String logFile;
    protected boolean verbose;

    protected String appHome;
    protected String conf;

    protected String sessionId;

    protected Configuration configuration;
    protected CatalogConfiguration catalogConfiguration;
    protected StorageConfiguration storageConfiguration;

    protected Logger logger;

    private static final String SESSION_FILENAME = "session.json";

    public CommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
//        init(options);
        init(options.logLevel, options.verbose, options.conf);
    }

    public CommandExecutor(String logLevel, boolean verbose, String conf) {
//        init(options);
        init(logLevel, verbose, conf);
    }

//    protected void init(GeneralCliOptions.CommonCommandOptions options) {
//        init(options.logLevel, options.verbose, options.conf);
//    }

    protected void init(String logLevel, boolean verbose, String conf) {
        this.logLevel = logLevel;
        this.verbose = verbose;
        this.conf = conf;

        /**
         * System property 'app.home' is automatically set up in opencga-storage.sh. If by any reason
         * this is 'null' then OPENCGA_HOME environment variable is used instead.
         */
        this.appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));
        Config.setOpenCGAHome(appHome);

        if (StringUtils.isEmpty(conf)) {
            this.conf = appHome + "/conf";
        }


        if (verbose) {
            logLevel = "debug";
        }

        if (logLevel != null && !logLevel.isEmpty()) {
            this.logLevel = logLevel;
            // We must call to this method
            configureDefaultLog(logLevel);
        }

        try {
            SessionFile sessionFile = loadSessionFile();
            if (sessionFile != null) {
                this.sessionId = sessionFile.getSessionId();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        // This updated the timestamp every time the command is finished
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    updateSessionTimestamp();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

//        loadConfigurations();
    }

    protected String getSessionId(AnalysisCliOptionsParser.AnalysisCommonCommandOptions commonOptions) {
        if (StringUtils.isBlank(commonOptions.sessionId)) {
            return sessionId;
        } else {
            return commonOptions.sessionId;
        }
    }


    public abstract void execute() throws Exception;

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
    }


    @Deprecated
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
     * This method attempts to load general configuration from CLI 'conf' parameter, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadOpencgaConfiguration() throws IOException {
        FileUtils.checkDirectory(Paths.get(this.conf));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.conf).resolve("configuration.yml");
        if (path != null && Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.configuration= Configuration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading configuration from JAR file");
            this.configuration = Configuration.load(ClientConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
        }
    }


    /**
     * This method attempts to load general configuration from CLI 'conf' parameter, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadCatalogConfiguration() throws IOException {
        FileUtils.checkDirectory(Paths.get(this.conf));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.conf).resolve("catalog-configuration.yml");
        if (path != null && Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.catalogConfiguration = CatalogConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading configuration from JAR file");
            this.catalogConfiguration = CatalogConfiguration
                    .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("catalog-configuration.yml"));
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

//        logger.debug("Loading configuration from '{}'", loadedConfigurationFile);
    }

    /**
     * This method attempts to load storage configuration from CLI 'conf' parameter, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadStorageConfiguration() throws IOException {
        FileUtils.checkDirectory(Paths.get(this.conf));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.conf).resolve("storage-configuration.yml");
        if (path != null && Files.exists(path)) {
            logger.debug("Loading storage configuration from '{}'", path.toAbsolutePath());
            this.storageConfiguration = StorageConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading storage configuration from JAR file");
            this.storageConfiguration = StorageConfiguration
                    .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
        }
    }


    protected void saveSessionFile(String user, String session) throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga");
        // check if ~/.opencga folder exists
        if (!Files.exists(sessionPath)) {
            Files.createDirectory(sessionPath);
        }
        sessionPath = sessionPath.resolve(SESSION_FILENAME);
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), new SessionFile(user, session));
    }

    protected SessionFile loadSessionFile() throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            return new ObjectMapper().readValue(sessionPath.toFile(), SessionFile.class);
        } else {
            return null;
        }
    }

    protected void updateSessionTimestamp() throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            ObjectMapper objectMapper = new ObjectMapper();
            SessionFile sessionFile = objectMapper.readValue(sessionPath.toFile(), SessionFile.class);
            sessionFile.setTimestamp(System.currentTimeMillis());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), sessionFile);
        }
    }

    protected void logoutSessionFile() throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            ObjectMapper objectMapper = new ObjectMapper();
            SessionFile sessionFile = objectMapper.readValue(sessionPath.toFile(), SessionFile.class);
            sessionFile.setLogout(LocalDateTime.now().toString());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), sessionFile);
        }
    }


    public CatalogConfiguration getCatalogConfiguration() {
        return catalogConfiguration;
    }

    public void setCatalogConfiguration(CatalogConfiguration catalogConfiguration) {
        this.catalogConfiguration = catalogConfiguration;
    }

    public String getLogLevel() {
        return logLevel;
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


    public String getConf() {
        return conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }

    public Logger getLogger() {
        return logger;
    }
}
