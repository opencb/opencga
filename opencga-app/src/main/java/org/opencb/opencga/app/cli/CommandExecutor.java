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

package org.opencb.opencga.app.cli;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 19/04/16.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    @Deprecated
    protected String logFile;

    protected String appHome;
    protected String conf;

    protected String userId;
    protected String token;

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;

    protected GeneralCliOptions.CommonCommandOptions options;

    protected Logger logger;
    private Logger privateLogger;

    public CommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        this.options = options;
        init(options.logLevel, options.conf);
    }

    protected void init(String logLevel, String conf) {
        this.logLevel = logLevel;
        this.conf = conf;

        /**
         * System property 'app.home' is automatically set up in opencga.sh. If by any reason
         * this is 'null' then OPENCGA_HOME environment variable is used instead.
         */
        this.appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));

        if (StringUtils.isEmpty(conf)) {
            this.conf = appHome + "/conf";
        }

        // Loggers can be initialized, the configuration happens just below these lines
        logger = LoggerFactory.getLogger(this.getClass().toString());
        privateLogger = LoggerFactory.getLogger(CommandExecutor.class);

        try {
            // At the moment this is needed for all three command lines, this might change soon since REST client should not need this one.
            loadConfiguration();

            // This code assumes general configuration will be always needed and general configuration is overwritten,
            // maybe in the near future this should be an if/else.

            if (StringUtils.isNotEmpty(ClientConfiguration.getInstance().getLogLevel())) {
                this.configuration.setLogLevel(ClientConfiguration.getInstance().getLogLevel());
            }

            // Do not change the order here, we can only configure logger after loading the configuration files,
            // this still relies on general configuration file.
            configureLogger();

            // Let's check the session file, maybe the session is still valid

            privateLogger.debug("CLI session file is: {}", CliSessionManager.getCurrentFile());

            if (StringUtils.isNotBlank(options.token)) {
                this.token = options.token;
            } else {
                this.token = CliSessionManager.getToken();
                this.userId = CliSessionManager.getUser();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
            // e.printStackTrace();
        }

        // Update the timestamp every time one executed command finishes
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                updateCliSessionFile();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }));
    }

    public abstract void execute() throws Exception;

    private void configureLogger() throws IOException {
        // Command line parameters have preference over configuration file
        // We overwrite logLevel configuration param with command line value
        if (StringUtils.isNotEmpty(this.logLevel)) {
            this.configuration.setLogLevel(this.logLevel);
        }

        Level level = Level.toLevel(configuration.getLogLevel(), Level.INFO);
        System.setProperty("opencga.log.level", level.name());
        Configurator.reconfigure();
    }

    @Deprecated
    public boolean loadConfigurations() {
        try {
            loadConfiguration();
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
     * This method attempts to load general configuration from CLI 'conf' parameter, if not exists then loads JAR configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadConfiguration() throws IOException {
        FileUtils.checkDirectory(Paths.get(this.conf));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.conf).resolve("configuration.yml");
        if (Files.exists(path)) {
            privateLogger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.configuration = Configuration.load(new FileInputStream(path.toFile()));
        } else {
            privateLogger.debug("Loading configuration from JAR file");
            this.configuration = Configuration
                    .load(Configuration.class.getClassLoader().getResourceAsStream("configuration.yml"));
        }
    }

    /**
     * This method attempts to load storage configuration from CLI 'conf' parameter, if not exists then loads JAR
     * storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadStorageConfiguration() throws IOException {
        FileUtils.checkDirectory(Paths.get(this.conf));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.conf).resolve("storage-configuration.yml");
        if (Files.exists(path)) {
            privateLogger.debug("Loading storage configuration from '{}'", path.toAbsolutePath());
            this.storageConfiguration = StorageConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            privateLogger.debug("Loading storage configuration from JAR file");
            this.storageConfiguration = StorageConfiguration
                    .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
        }
    }

    protected void checkSignatureRelease(String release) throws ClientException {
        switch (release) {
            case "2":
            case "3":
            case "3.1":
            case "3.2":
                break;
            default:
                throw new ClientException("Invalid value " + release + " for the mutational signature release. "
                        + "Valid values are: 2, 3, 3.1 and 3.2");
        }
    }

    public static String getParsedSubCommand(JCommander jCommander) {
        return CliOptionsParser.getSubCommand(jCommander);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
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
