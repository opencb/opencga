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
    protected String conf;

    protected String appHome;
    protected String userId;
    protected String token;

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected ClientConfiguration clientConfiguration;

    protected GeneralCliOptions.CommonCommandOptions options;

    protected Logger logger;
    private Logger privateLogger;

    public CommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean loadClientConfiguration) {
        this.options = options;

        init(options.logLevel, options.conf, loadClientConfiguration);
    }

    protected void init(String logLevel, String conf, boolean loadClientConfiguration) {
        this.logLevel = logLevel;
        this.conf = conf;

        // System property 'app.home' is automatically set up in opencga.sh. If by any reason
        // this is 'null' then OPENCGA_HOME environment variable is used instead.
        this.appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));

        if (StringUtils.isEmpty(conf)) {
            this.conf = appHome + "/conf";
        }

        // Loggers can be initialized, the configuration happens just below these lines
        logger = LoggerFactory.getLogger(this.getClass().toString());
        privateLogger = LoggerFactory.getLogger(CommandExecutor.class);

        try {
            // FIXME This is not needed for the client command line,
            //  this class needs to be refactor in next release 2.3.0
            loadConfiguration();
            loadStorageConfiguration();

            // client configuration is only loaded under demand
            if (loadClientConfiguration) {
                loadClientConfiguration();
            }

            // Do not change the order here, we can only configure logger after loading the configuration files,
            // this still relies on general configuration file.
            configureLogger();

            // Let's check the session file, maybe the session is still valid
            privateLogger.debug("CLI session file is: {}", CliSessionManager.getInstance().getCurrentFile());

            if (StringUtils.isNotBlank(options.token)) {
                this.token = options.token;
            } else {
                this.token = CliSessionManager.getInstance().getToken();
                this.userId = CliSessionManager.getInstance().getUser();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    public abstract void execute() throws Exception;

    public static String getParsedSubCommand(JCommander jCommander) {
        return CliOptionsParser.getSubCommand(jCommander);
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

    /**
     * This method attempts to first data configuration from CLI parameter, if not present then uses
     * the configuration from installation directory, if not exists then loads JAR client-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadClientConfiguration() throws IOException {
        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.conf).resolve("client-configuration.yml");
        if (Files.exists(path)) {
            privateLogger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.clientConfiguration = ClientConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            privateLogger.debug("Loading configuration from JAR file");
            this.clientConfiguration = ClientConfiguration
                    .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("client-configuration.yml"));
        }
    }

    public String getLogLevel() {
        return logLevel;
    }

    public CommandExecutor setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getConf() {
        return conf;
    }

    public CommandExecutor setConf(String conf) {
        this.conf = conf;
        return this;
    }

    public String getAppHome() {
        return appHome;
    }

    public CommandExecutor setAppHome(String appHome) {
        this.appHome = appHome;
        return this;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public CommandExecutor setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public CommandExecutor setStorageConfiguration(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
        return this;
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    public CommandExecutor setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        return this;
    }
}
