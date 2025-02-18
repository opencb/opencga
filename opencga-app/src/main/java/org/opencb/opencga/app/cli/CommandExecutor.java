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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.generator.models.RestParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 19/04/16.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    protected String conf;

    protected String appHome;
    protected Path opencgaHome;
    protected String userId;
    protected String token;

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected ClientConfiguration clientConfiguration;

    protected String host;
    protected SessionManager sessionManager;

    protected GeneralCliOptions.CommonCommandOptions options;

    protected Logger logger;
    private Logger privateLogger;

    public CommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean loadClientConfiguration) {
        this.options = options;

        init(options.logLevel, options.conf, loadClientConfiguration);
    }

    public static String getParsedSubCommand(JCommander jCommander) {
        return CliOptionsParser.getSubCommand(jCommander);
    }

    private static void configureLogger(String logLevel) throws IOException {
        // Command line parameters have preference over anything
        if (StringUtils.isNotBlank(logLevel)) {
            Level level = Level.toLevel(logLevel);
            System.setProperty("opencga.log.level", level.name());
            Configurator.reconfigure();
        }
    }

    protected void init(String logLevel, String conf, boolean loadClientConfiguration) {
        this.logLevel = logLevel;
        this.conf = conf;

        // System property 'app.home' is automatically set up in opencga.sh. If by any reason
        // this is 'null' then OPENCGA_HOME environment variable is used instead.
        this.appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));
        this.opencgaHome = Paths.get(appHome);

        if (StringUtils.isEmpty(conf)) {
            this.conf = appHome + "/conf";
        }

        // Loggers can be initialized, the configuration happens just below these lines
        logger = LoggerFactory.getLogger(this.getClass().toString());
        privateLogger = LoggerFactory.getLogger(CommandExecutor.class);

        try {
            configureLogger(this.logLevel);

            // FIXME This is not needed for the client command line,
            //  this class needs to be refactor in next release 2.3.0
            loadConfiguration();
            loadStorageConfiguration();

            // client configuration is only loaded under demand
            if (loadClientConfiguration) {
                loadClientConfiguration();
            }

            // We need to check if parameter --host has been provided.
            // Then set the host and make it the default
            if (StringUtils.isNotEmpty(options.host)) {
                this.host = options.host;
                try {
                    clientConfiguration.setDefaultIndexByName(this.host);
                } catch (Exception e) {
                    PrintUtils.printError("Invalid host " + host);
                    System.exit(-1);
                }
            } else {
                this.host = clientConfiguration.getCurrentHost().getName();
            }
            // Create the SessionManager and store current session
            sessionManager = new SessionManager(clientConfiguration, this.host);

            // Let's check the session file, maybe the session is still valid
//            privateLogger.debug("CLI session file is: {}", CliSessionManager.getInstance().getCurrentFile());
            privateLogger.debug("CLI session file is: {}", this.sessionManager.getSessionPath(this.host).toString());

            if (StringUtils.isNotBlank(options.token)) {
                this.token = options.token;
            } else {
//                this.token = CliSessionManager.getInstance().getToken();
//                this.userId = CliSessionManager.getInstance().getUser();
                this.token = sessionManager.getSession().getToken();
                this.userId = sessionManager.getSession().getUser();

            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClientException e) {
            e.printStackTrace();
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

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public CommandExecutor setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }

    public String getObjectAsJSON(String objectCategory, String objectPath, OpenCGAClient openCGAClient) throws Exception {
        StringBuilder jsonInString = new StringBuilder("\n");
        try {
            ObjectMap queryParams = new ObjectMap();
            queryParams.putIfNotEmpty("category", objectCategory);
            RestResponse<List> response = openCGAClient.getMetaClient().api(queryParams);
            ObjectMapper jsonObjectMapper = new ObjectMapper();
            boolean found = false;
            for (List list : response.getResponses().get(0).getResults()) {
                List<RestCategory> categories = jsonObjectMapper.convertValue(list, new TypeReference<List<RestCategory>>() {});
                for (RestCategory category : categories) {
                    for (RestEndpoint endpoint : category.getEndpoints()) {
                        if (objectPath.equals(endpoint.getPath())) {
                            for (RestParameter parameter : endpoint.getParameters()) {
                                if (parameter.getData() != null) {
                                    found = true;
                                    Map<String, Object> map = getExampleBody(parameter.getData());
                                    jsonInString.append(jsonObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(map));
                                }
                            }
                        }
                    }
                }
            }
            if (!found) {
                jsonInString.append("No model available");
            }
        } catch (Exception e) {
            jsonInString = new StringBuilder("Data model not found.");
            CommandLineUtils.error(e);
        }
        return jsonInString.toString();
    }

    private Map<String, Object> getExampleBody(List<RestParameter> data) {
        Map<String, Object> result = new HashMap<>();
        for (RestParameter parameter : data) {
            if (parameter.getData() == null) {
                result.put(parameter.getName(), getParameterExampleValue(parameter));
            } else {
                result.put(parameter.getName(), getExampleBody(parameter.getData()));
            }
        }
       return result;
    }

    private Object getParameterExampleValue(RestParameter parameter) {
        if(!StringUtils.isEmpty(parameter.getAllowedValues())){
            return parameter.getAllowedValues().replace(" ", "|");
        }

        switch (parameter.getType()) {
            case "Boolean":
            case "java.lang.Boolean":
                return false;
            case "Long":
            case "Float":
            case "Double":
            case "Integer":
            case "int":
            case "double":
            case "float":
            case "long":
                return 0;
            case "List":
                return Collections.singletonList("");
            case "Date":
                return "dd/mm/yyyy";
            case "Map":
                return Collections.singletonMap("key", "value");
            case "String":
                return "";
            default:
                logger.debug("Unknown type: " + parameter.getType() + " for parameter: " + parameter.getName());
                return "-";
        }
    }

    public Logger getLogger() {
        return logger;
    }

}
