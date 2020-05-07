/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * Created by imedina on 16/03/16.
 */
public class Configuration {

    private String logLevel;
    private String logFile;

    private boolean openRegister;

    private String databasePrefix;
    private String workspace;
    private String jobDir;

    private Admin admin;
    private Monitor monitor;
    private HealthCheck healthCheck;
    private Audit audit;

    private Map<String, Map<String, List<HookConfiguration>>> hooks;

    private Email email;
    private Catalog catalog;
    private Analysis analysis;
    private Panel panel;

    private ServerConfiguration server;
    private Authentication authentication;

    private static Logger logger;

    private static final String DEFAULT_CONFIGURATION_FORMAT = "yaml";

    static {
        logger = LoggerFactory.getLogger(Configuration.class);
    }

    public Configuration() {
        admin = new Admin();
        monitor = new Monitor();
        healthCheck = new HealthCheck();
        audit = new Audit();
        hooks = new HashMap<>();
        email = new Email();
        catalog = new Catalog();
        analysis = new Analysis();
        panel = new Panel();
        server = new ServerConfiguration();
        authentication = new Authentication();
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        yamlMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    public static Configuration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, DEFAULT_CONFIGURATION_FORMAT);
    }

    public static Configuration load(InputStream configurationInputStream, String format) throws IOException {
        if (configurationInputStream == null) {
            throw new IOException("Configuration file not found");
        }
        Configuration configuration;
        ObjectMapper objectMapper;
        //TODO : create mandatory fields check to avoid invalid or incomplete conf
        try {
            switch (format) {
                case "json":
                    objectMapper = new ObjectMapper();
                    configuration = objectMapper.readValue(configurationInputStream, Configuration.class);
                    break;
                case "yml":
                case "yaml":
                default:
                    objectMapper = new ObjectMapper(new YAMLFactory());
                    configuration = objectMapper.readValue(configurationInputStream, Configuration.class);
                    break;
            }
        } catch (IOException e) {
            throw new IOException("Configuration file could not be parsed: " + e.getMessage(), e);
        }

        // We must always overwrite configuration with environment parameters
        overwriteWithEnvironmentVariables(configuration);
        return configuration;
    }

    private static void overwriteWithEnvironmentVariables(Configuration configuration) {
        Map<String, String> envVariables = System.getenv();
        for (String variable : envVariables.keySet()) {
            if (variable.startsWith("OPENCGA_")) {
                logger.debug("Overwriting environment parameter '{}'", variable);
                switch (variable) {
                    case "OPENCGA_DB_PREFIX":
                        configuration.setDatabasePrefix(envVariables.get(variable));
                        break;
                    case "OPENCGA_USER_WORKSPACE":
                        configuration.setWorkspace(envVariables.get(variable));
                        break;
                    case "OPENCGA_MONITOR_PORT":
                        configuration.getMonitor().setPort(Integer.parseInt(envVariables.get(variable)));
                        break;
                    case "OPENCGA_EXECUTION_MODE":
                    case "OPENCGA_EXECUTION_ID":
                        configuration.getAnalysis().getExecution().setId(envVariables.get(variable));
                        break;
                    case "OPENCGA_MAIL_HOST":
                        configuration.getEmail().setHost(envVariables.get(variable));
                        break;
                    case "OPENCGA_MAIL_PORT":
                        configuration.getEmail().setPort(envVariables.get(variable));
                        break;
                    case "OPENCGA_MAIL_USER":
                        configuration.getEmail().setUser(envVariables.get(variable));
                        break;
                    case "OPENCGA_MAIL_PASSWORD":
                        configuration.getEmail().setPassword(envVariables.get(variable));
                        break;
                    case "OPENCGA_CATALOG_DB_HOSTS":
                        configuration.getCatalog().getDatabase().setHosts(Arrays.asList(envVariables.get(variable).split(",")));
                        break;
                    case "OPENCGA_CATALOG_DB_USER":
                        configuration.getCatalog().getDatabase().setUser(envVariables.get(variable));
                        break;
                    case "OPENCGA_CATALOG_DB_PASSWORD":
                        configuration.getCatalog().getDatabase().setPassword(envVariables.get(variable));
                        break;
                    case "OPENCGA_CATALOG_DB_AUTHENTICATION_DATABASE":
                        configuration.getCatalog().getDatabase().getOptions().put("authenticationDatabase", envVariables.get(variable));
                        break;
                    case "OPENCGA_CATALOG_DB_CONNECTIONS_PER_HOST":
                        configuration.getCatalog().getDatabase().getOptions().put("connectionsPerHost", envVariables.get(variable));
                        break;
                    case "OPENCGA_CATALOG_SEARCH_HOST":
                        configuration.getCatalog().getSearchEngine().setHosts(Collections.singletonList(envVariables.get(variable)));
                        break;
                    case "OPENCGA_CATALOG_SEARCH_TIMEOUT":
                        configuration.getCatalog().getSearchEngine().getOptions().put("timeout", envVariables.get(variable));
                        break;
                    case "OPENCGA_CATALOG_SEARCH_BATCH":
                        configuration.getCatalog().getSearchEngine().getOptions().put("insertBatchSize", envVariables.get(variable));
                        break;
                    case "OPENCGA_SERVER_REST_PORT":
                        configuration.getServer().getRest().setPort(Integer.parseInt(envVariables.get(variable)));
                        break;
                    case "OPENCGA_SERVER_GRPC_PORT":
                        configuration.getServer().getGrpc().setPort(Integer.parseInt(envVariables.get(variable)));
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", openRegister=").append(openRegister);
        sb.append(", databasePrefix='").append(databasePrefix).append('\'');
        sb.append(", workspace='").append(workspace).append('\'');
        sb.append(", admin=").append(admin);
        sb.append(", monitor=").append(monitor);
        sb.append(", audit=").append(audit);
        sb.append(", hooks=").append(hooks);
        sb.append(", email=").append(email);
        sb.append(", catalog=").append(catalog);
        sb.append(", panel=").append(panel);
        sb.append(", server=").append(server);
        sb.append(", authentication=").append(authentication);
        sb.append('}');
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public Configuration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getLogFile() {
        return logFile;
    }

    public Configuration setLogFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

    public boolean isOpenRegister() {
        return openRegister;
    }

    public Configuration setOpenRegister(boolean openRegister) {
        this.openRegister = openRegister;
        return this;
    }

    public String getDatabasePrefix() {
        return databasePrefix;
    }

    public Configuration setDatabasePrefix(String databasePrefix) {
        this.databasePrefix = databasePrefix;
        return this;
    }

    public String getWorkspace() {
        return workspace;
    }

    public Configuration setWorkspace(String workspace) {
        this.workspace = workspace;
        return this;
    }

    public String getJobDir() {
        return jobDir;
    }

    public Configuration setJobDir(String jobDir) {
        this.jobDir = jobDir;
        return this;
    }

    public Admin getAdmin() {
        return admin;
    }

    public Configuration setAdmin(Admin admin) {
        this.admin = admin;
        return this;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public Configuration setMonitor(Monitor monitor) {
        this.monitor = monitor;
        return this;
    }

    @Deprecated
    public Execution getExecution() {
        return getAnalysis().getExecution();
    }

    @Deprecated
    public Configuration setExecution(Execution execution) {
        getAnalysis().setExecution(execution);
        return this;
    }

    public Map<String, Map<String, List<HookConfiguration>>> getHooks() {
        return hooks;
    }

    public Configuration setHooks(Map<String, Map<String, List<HookConfiguration>>> hooks) {
        this.hooks = hooks;
        return this;
    }

    public Email getEmail() {
        return email;
    }

    public Configuration setEmail(Email email) {
        this.email = email;
        return this;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public Configuration setCatalog(Catalog catalog) {
        this.catalog = catalog;
        return this;
    }

    public Analysis getAnalysis() {
        return analysis;
    }

    public Configuration setAnalysis(Analysis analysis) {
        this.analysis = analysis;
        return this;
    }

    public Audit getAudit() {
        return audit;
    }

    public Configuration setAudit(Audit audit) {
        this.audit = audit;
        return this;
    }

    public Panel getPanel() {
        return panel;
    }

    public Configuration setPanel(Panel panel) {
        this.panel = panel;
        return this;
    }

    public ServerConfiguration getServer() {
        return server;
    }

    public Configuration setServer(ServerConfiguration server) {
        this.server = server;
        return this;
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }

    public HealthCheck getHealthCheck() {
        return healthCheck;
    }

    public Configuration setHealthCheck(HealthCheck healthCheck) {
        this.healthCheck = healthCheck;
        return this;
    }
}
