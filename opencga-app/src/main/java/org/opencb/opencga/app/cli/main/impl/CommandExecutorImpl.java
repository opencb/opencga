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

package org.opencb.opencga.app.cli.main.impl;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.app.cli.AbstractCommandExecutor;
import org.opencb.commons.app.cli.GeneralCliOptions;
import org.opencb.commons.app.cli.session.AbstractSessionManager;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 19/04/16.
 */
public abstract class CommandExecutorImpl extends AbstractCommandExecutor {


    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected ClientConfiguration clientConfiguration;


    public CommandExecutorImpl(GeneralCliOptions.CommonCommandOptions options, boolean loadClientConfiguration) {
        super(options, loadClientConfiguration);
        this.options = options;

        init(options.logLevel, options.conf, loadClientConfiguration);
    }


    public AbstractSessionManager configureSession() {
        try {
            if (StringUtils.isNotEmpty(options.host)) {
                this.host = options.host;
                clientConfiguration.setDefaultIndexByName(this.host);
            } else {
                this.host = clientConfiguration.getCurrentHost().getName();
            }
        } catch (ClientException e) {
            e.printStackTrace();
        }
        // Create the SessionManager and store current session
        return new SessionManagerImpl(clientConfiguration, this.host);
    }

    public abstract void execute() throws Exception;

    public void loadConf(boolean loadClientConfiguration) throws IOException {
        // FIXME This is not needed for the client command line,
        //  this class needs to be refactor in next release 2.3.0
        loadConfiguration();
        loadStorageConfiguration();

        // client configuration is only loaded under demand
        if (loadClientConfiguration) {
            loadClientConfiguration();
        }
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
            logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.configuration = Configuration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading configuration from JAR file");
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
            logger.debug("Loading storage configuration from '{}'", path.toAbsolutePath());
            this.storageConfiguration = StorageConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading storage configuration from JAR file");
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
            logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.clientConfiguration = ClientConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading configuration from JAR file");
            this.clientConfiguration = ClientConfiguration
                    .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("client-configuration.yml"));
        }
    }

    public String getLogLevel() {
        return logLevel;
    }

    public CommandExecutorImpl setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getConf() {
        return conf;
    }

    public CommandExecutorImpl setConf(String conf) {
        this.conf = conf;
        return this;
    }

    public String getAppHome() {
        return appHome;
    }

    public CommandExecutorImpl setAppHome(String appHome) {
        this.appHome = appHome;
        return this;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public CommandExecutorImpl setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public CommandExecutorImpl setStorageConfiguration(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
        return this;
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    public CommandExecutorImpl setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        return this;
    }

    public AbstractSessionManager getSessionManager() {
        return sessionManager;
    }

    public CommandExecutorImpl setSessionManager(SessionManagerImpl sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }


}
