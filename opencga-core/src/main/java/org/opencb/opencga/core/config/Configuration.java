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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.models.externalTool.WorkflowSystem;
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

    /**
     * Service log level. Used for the REST and MASTER services.
     */
    private String logLevel;
    /**
     * Service log dir. Used for the REST and MASTER services.
     */
    private String logDir;

    private String databasePrefix;
    private String workspace;
    private String jobDir;

    private AccountConfiguration account;
    private QuotaConfiguration quota;

    private Monitor monitor;
    private HealthCheck healthCheck;
    private Audit audit;

    private Map<String, Map<String, List<HookConfiguration>>> hooks;

    private Email email;
    private Catalog catalog;
    private Analysis analysis;
    private Panel panel;

    private ServerConfiguration server;

    private static final Set<String> reportedFields = new HashSet<>();

    private static final Logger logger;

    private static final String DEFAULT_CONFIGURATION_FORMAT = "yaml";

    static {
        logger = LoggerFactory.getLogger(Configuration.class);
    }

    public Configuration() {
        monitor = new Monitor();
        healthCheck = new HealthCheck();
        audit = new Audit();
        hooks = new HashMap<>();
        email = new Email();
        catalog = new Catalog();
        analysis = new Analysis();
        panel = new Panel();
        server = new ServerConfiguration();
        account = new AccountConfiguration();
        quota = QuotaConfiguration.init();
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
        addDefaultValueIfMissing(configuration);

        // We must always overwrite configuration with environment parameters
        overwriteWithEnvironmentVariables(configuration);
        return configuration;
    }

    private static void addDefaultValueIfMissing(Configuration configuration) {
        if (configuration.getAccount() == null) {
            configuration.setAccount(new AccountConfiguration());
        }
        if (configuration.getAccount().getMaxLoginAttempts() <= 0) {
            configuration.getAccount().setMaxLoginAttempts(5);
        }
        if (configuration.getAccount().getPasswordExpirationDays() < 0) {
            // Disable password expiration by default
            configuration.getAccount().setPasswordExpirationDays(0);
        }

        if (configuration.getQuota() == null) {
            configuration.setQuota(QuotaConfiguration.init());
        }

        if (configuration.getAnalysis().getWorkflow() == null) {
            configuration.getAnalysis().setWorkflow(new WorkflowConfiguration());
        }
        addDefaultAnalysisWorkflowValues(configuration.getAnalysis().getWorkflow());
    }

    private static void addDefaultAnalysisWorkflowValues(WorkflowConfiguration workflowConfiguration) {
        if (CollectionUtils.isEmpty(workflowConfiguration.getManagers())) {
            workflowConfiguration.setManagers(Collections.singletonList(
                    new WorkflowSystemConfiguration(WorkflowSystem.SystemId.NEXTFLOW.name(), ParamConstants.DEFAULT_MIN_NEXTFLOW_VERSION)));
        }
        if (workflowConfiguration.getMinRequirements() == null) {
            workflowConfiguration.setMinRequirements(new MinimumRequirements());
        }
        MinimumRequirements minRequirements = workflowConfiguration.getMinRequirements();
        if (StringUtils.isEmpty(minRequirements.getCpu())) {
            minRequirements.setCpu("2");
        }
        if (StringUtils.isEmpty(minRequirements.getMemory())) {
            minRequirements.setMemory("8"); // GB
        }
        if (StringUtils.isEmpty(minRequirements.getDisk())) {
            minRequirements.setDisk("100"); // GB
        }
    }

    private static void overwriteWithEnvironmentVariables(Configuration configuration) {
        Map<String, String> envVariables = System.getenv();
        for (String variable : envVariables.keySet()) {
            if (variable.startsWith("OPENCGA_")) {
                logger.debug("Overwriting environment parameter '{}'", variable);
                String value = envVariables.get(variable);
                switch (variable) {
                    case "OPENCGA_LOG_DIR":
                        configuration.setLogDir(value);
                        break;
                    case "OPENCGA_DB_PREFIX":
                        configuration.setDatabasePrefix(value);
                        break;
                    case "OPENCGA_USER_WORKSPACE":
                        configuration.setWorkspace(value);
                        break;
                    case "OPENCGA_MONITOR_PORT":
                        configuration.getMonitor().setPort(Integer.parseInt(value));
                        break;
                    case "OPENCGA.MAX_LOGIN_ATTEMPTS":
                    case "OPENCGA.ACCOUNT.MAX_LOGIN_ATTEMPTS":
                        configuration.getAccount().setMaxLoginAttempts(Integer.parseInt(value));
                        break;
                    case "OPENCGA_EXECUTION_MODE":
                    case "OPENCGA_EXECUTION_ID":
                        configuration.getAnalysis().getExecution().setId(value);
                        break;
                    case "OPENCGA_MAIL_HOST":
                        configuration.getEmail().setHost(value);
                        break;
                    case "OPENCGA_MAIL_PORT":
                        configuration.getEmail().setPort(value);
                        break;
                    case "OPENCGA_MAIL_USER":
                        configuration.getEmail().setUser(value);
                        break;
                    case "OPENCGA_MAIL_PASSWORD":
                        configuration.getEmail().setPassword(value);
                        break;
                    case "OPENCGA_CATALOG_DB_HOSTS":
                        configuration.getCatalog().getDatabase().setHosts(Arrays.asList(value.split(",")));
                        break;
                    case "OPENCGA_CATALOG_DB_USER":
                        configuration.getCatalog().getDatabase().setUser(value);
                        break;
                    case "OPENCGA_CATALOG_DB_PASSWORD":
                        configuration.getCatalog().getDatabase().setPassword(value);
                        break;
                    case "OPENCGA_CATALOG_DB_AUTHENTICATION_DATABASE":
                        configuration.getCatalog().getDatabase().getOptions().put("authenticationDatabase", value);
                        break;
                    case "OPENCGA_CATALOG_DB_CONNECTIONS_PER_HOST":
                        configuration.getCatalog().getDatabase().getOptions().put("connectionsPerHost", value);
                        break;
                    case "OPENCGA_SERVER_REST_PORT":
                        configuration.getServer().getRest().setPort(Integer.parseInt(value));
                        break;
                    case "OPENCGA_SERVER_GRPC_PORT":
                        configuration.getServer().getGrpc().setPort(Integer.parseInt(value));
                        break;
                    default:
                        break;
                }
            }
        }
    }

    public static void reportUnusedField(String field, Object value) {
        // Report only if the value is not null and not an empty string
        if (value != null && !(value instanceof String && ((String) value).isEmpty())) {
            if (reportedFields.add(field)) {
                // Only log the first time a field is found
                logger.warn("Ignored configuration option '{}' with value '{}'. The option was deprecated and removed.", field, value);
            }
        }
    }

    public static void reportMovedField(String previousField, String newField, Object value) {
        // Report only if the value is not null and not an empty string
        if (value != null && !(value instanceof String && ((String) value).isEmpty())) {
            if (reportedFields.add(previousField)) {
                // Only log the first time a field is found
                logger.warn("Option '{}' with value '{}' was moved to '{}'.", previousField, value, newField);
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", logDir='").append(logDir).append('\'');
        sb.append(", databasePrefix='").append(databasePrefix).append('\'');
        sb.append(", workspace='").append(workspace).append('\'');
        sb.append(", jobDir='").append(jobDir).append('\'');
        sb.append(", account=").append(account);
        sb.append(", quota=").append(quota);
        sb.append(", monitor=").append(monitor);
        sb.append(", healthCheck=").append(healthCheck);
        sb.append(", audit=").append(audit);
        sb.append(", hooks=").append(hooks);
        sb.append(", email=").append(email);
        sb.append(", catalog=").append(catalog);
        sb.append(", analysis=").append(analysis);
        sb.append(", panel=").append(panel);
        sb.append(", server=").append(server);
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

    public String getLogDir() {
        return logDir;
    }

    public Configuration setLogDir(String logDir) {
        this.logDir = logDir;
        return this;
    }

    @Deprecated
    public String getLogFile() {
        return null;
    }

    @Deprecated
    public Configuration setLogFile(String logFile) {
        reportUnusedField("configuration.yml#logFile", logFile);
        return this;
    }

    @Deprecated
    public Boolean isOpenRegister() {
        return null;
    }

    @Deprecated
    public Configuration setOpenRegister(boolean openRegister) {
        reportUnusedField("configuration.yml#openRegister", openRegister);
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

    public AccountConfiguration getAccount() {
        return account;
    }

    public Configuration setAccount(AccountConfiguration account) {
        this.account = account;
        return this;
    }

    public QuotaConfiguration getQuota() {
        return quota;
    }

    public Configuration setQuota(QuotaConfiguration quota) {
        this.quota = quota;
        return this;
    }

    @Deprecated
    public int getMaxLoginAttempts() {
        return account.getMaxLoginAttempts();
    }

    @Deprecated
    public Configuration setMaxLoginAttempts(int maxLoginAttempts) {
        reportMovedField("configuration.yml#maxLoginAttempts", "configuration.yml#account.maxLoginAttempts", maxLoginAttempts);
        account.setMaxLoginAttempts(maxLoginAttempts);
        return this;
    }

    @Deprecated
    public Admin getAdmin() {
        return null;
    }

    @Deprecated
    public Configuration setAdmin(Admin admin) {
        reportUnusedField("configuration.yml#admin", admin);
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

    @Deprecated
    public Optimizations getOptimizations() {
        return null;
    }

    @Deprecated
    public Configuration setOptimizations(Optimizations optimizations) {
        reportUnusedField("configuration.yml#optimizations", optimizations);
        return this;
    }

    public ServerConfiguration getServer() {
        return server;
    }

    public Configuration setServer(ServerConfiguration server) {
        this.server = server;
        return this;
    }

    @Deprecated
    public Authentication getAuthentication() {
        return null;
    }

    @Deprecated
    public Configuration setAuthentication(Authentication authentication) {
        reportUnusedField("configuration.yml#authentication", authentication);
        return this;
    }

    public HealthCheck getHealthCheck() {
        return healthCheck;
    }

    public Configuration setHealthCheck(HealthCheck healthCheck) {
        this.healthCheck = healthCheck;
        return this;
    }
}
