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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.RollingFileAppender;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 19/04/16.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    @Deprecated
    protected String logFile;
    protected boolean verbose;

    protected String appHome;
    protected String conf;

    @Deprecated
    protected String userId;
    @Deprecated
    protected String sessionId;
    @Nullable
    protected CliSession cliSession;

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected ClientConfiguration clientConfiguration;

    protected GeneralCliOptions.CommonCommandOptions options;

    protected Logger logger;
    private Logger privateLogger;

    private static final String SESSION_FILENAME = "session.json";

    public CommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        this(options, false);
    }

    public CommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean loadClientConfiguration) {
        this.options = options;
        init(options.logLevel, options.verbose, options.conf, loadClientConfiguration);
    }

    @Deprecated
    public CommandExecutor(String logLevel, boolean verbose, String conf) {
        init(logLevel, verbose, conf, true);
    }

    protected void init(String logLevel, boolean verbose, String conf, boolean loadClientConfiguration) {
        this.logLevel = logLevel;
        this.verbose = verbose;
        this.conf = conf;

        /**
         * System property 'app.home' is automatically set up in opencga.sh. If by any reason
         * this is 'null' then OPENCGA_HOME environment variable is used instead.
         */
        this.appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));

        if (StringUtils.isEmpty(conf)) {
            this.conf = appHome + "/conf";
        }

        // At the moment verbose CLI param acts as a debug log level
        if (verbose) {
            this.logLevel = "debug";
        }

        // Loggers can be initialized, the configuration happens just below these lines
        logger = LoggerFactory.getLogger(this.getClass().toString());
        privateLogger = LoggerFactory.getLogger(CommandExecutor.class);

        try {
            // At the moment this is needed for all three command lines, this might change soon since REST client should not need this one.
            loadConfiguration();

            // This code assumes general configuration will be always needed and general configuration is overwritten,
            // maybe in the near future this should be an if/else.
            if (loadClientConfiguration) {
                loadClientConfiguration();
                if (StringUtils.isNotEmpty(this.clientConfiguration.getLogLevel())) {
                    this.configuration.setLogLevel(this.clientConfiguration.getLogLevel());
                }
                if (StringUtils.isNotEmpty(this.clientConfiguration.getLogFile())) {
                    this.configuration.setLogFile(this.clientConfiguration.getLogFile());
                }
            }

            // Do not change the order here, we can only configure logger after loading the configuration files,
            // this still relies on general configuration file.
            configureLogger();

            // Let's check the session file, maybe the session is still valid
            loadCliSessionFile();
            privateLogger.debug("CLI session file is: {}", this.cliSession);

            if (cliSession != null) {
                this.sessionId = cliSession.getToken();
                this.userId = cliSession.getUser();
            }

        } catch (IOException e) {
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

    @Deprecated
    protected String getSessionId(GeneralCliOptions.CommonCommandOptions commonOptions) {
        if (StringUtils.isBlank(commonOptions.sessionId) && cliSession != null) {
            return cliSession.getToken();
        } else {
            return commonOptions.sessionId;
        }
    }


    public abstract void execute() throws Exception;

    private void configureLogger() throws IOException {
        org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();

        // Disable MongoDB useless logging
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.cluster").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.connection").setLevel(Level.WARN);

        // Command line parameters have preference over configuration file
        // We overwrite logLevel configuration param with command line value
        if (StringUtils.isNotEmpty(this.logLevel)) {
            this.configuration.setLogLevel(this.logLevel);
        }

        // We overwrite logFile configuration param with command line value
        if (StringUtils.isNotEmpty(this.options.logFile)) {
            this.configuration.setLogFile(this.options.logFile);
        }

        Level level = Level.toLevel(configuration.getLogLevel(), Level.INFO);
        rootLogger.setLevel(level);

        // Configure the logger output, this can be the console or a file if provided by CLI or by configuration file
        ConsoleAppender stderr = (ConsoleAppender) rootLogger.getAppender("stderr");
        if (StringUtils.isEmpty(this.configuration.getLogFile())) {
            stderr.setThreshold(level);
        } else {
            RollingFileAppender rollingFileAppender = new RollingFileAppender(stderr.getLayout(), this.configuration.getLogFile(), true);
            rootLogger.addAppender(rollingFileAppender);
            rollingFileAppender.setThreshold(level);
            rollingFileAppender.setMaxFileSize("100MB");
            rollingFileAppender.setMaxBackupIndex(10);
        }
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
        if (path != null && Files.exists(path)) {
            privateLogger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.configuration = Configuration.load(new FileInputStream(path.toFile()));
        } else {
            privateLogger.debug("Loading configuration from JAR file");
            this.configuration = Configuration
                    .load(Configuration.class.getClassLoader().getResourceAsStream("configuration.yml"));
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
        if (path != null && Files.exists(path)) {
            privateLogger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.clientConfiguration = ClientConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            privateLogger.debug("Loading configuration from JAR file");
            this.clientConfiguration = ClientConfiguration
                    .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("client-configuration.yml"));
        }
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
            privateLogger.debug("Loading storage configuration from '{}'", path.toAbsolutePath());
            this.storageConfiguration = StorageConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            privateLogger.debug("Loading storage configuration from JAR file");
            this.storageConfiguration = StorageConfiguration
                    .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
        }
    }


    protected void loadCliSessionFile() {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            try {
                this.cliSession = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .readValue(sessionPath.toFile(), CliSession.class);
            } catch (IOException e) {
                privateLogger.debug("Could not parse the session file properly");
            }
        }
    }

    protected void saveCliSessionFile(String user, String session, List<String> studies) throws IOException {
        // Check the home folder exists
        if (!Files.exists(Paths.get(System.getProperty("user.home")))) {
            System.out.println("WARNING: Could not store token. User home folder '" + System.getProperty("user.home")
                    + "' not found. Please, manually provide the token for any following command lines with '-S {token}'.");
            return;
        }

        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga");
        // check if ~/.opencga folder exists
        if (!Files.exists(sessionPath)) {
            Files.createDirectory(sessionPath);
        }
        sessionPath = sessionPath.resolve(SESSION_FILENAME);
        CliSession cliSession = new CliSession(clientConfiguration.getRest().getHost(), user, session, studies);

        // we remove the part where the token signature is to avoid key verification
        int i = session.lastIndexOf('.');
        String withoutSignature = session.substring(0, i+1);
        Date expiration = Jwts.parser().parseClaimsJwt(withoutSignature).getBody().getExpiration();

        cliSession.setExpirationTime(TimeUtils.getTime(expiration));

        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), cliSession);
    }

    protected void updateCliSessionFile() throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), cliSession);
        }
    }

//    protected void updateCliSessionFileTimestamp() throws IOException {
////        QueryResponse<ObjectMap> refresh = new OpenCGAClient(sessionId, clientConfiguration).refresh();
//
//        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
//        if (Files.exists(sessionPath)) {
//            ObjectMapper objectMapper = new ObjectMapper();
//            CliSession cliSession = objectMapper.readValue(sessionPath.toFile(), CliSession.class);
//            cliSession.setTimestamp(System.currentTimeMillis());
//            objectMapper.writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), cliSession);
//        }
//    }

    protected void logoutCliSessionFile() throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            Files.delete(sessionPath);
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
