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

package org.opencb.opencga.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 16/03/16.
 */
public class Configuration {

    private String logLevel;
    private String logDir;

    private boolean openRegister;
    private int userDefaultQuota;

    private String databasePrefix;
    private String dataDir;
    private String tempJobsDir;
    private String toolDir;

    private Admin admin;
    private Monitor monitor;
    private Execution execution;
    private Audit audit;

    private Map<String, Map<String, List<HookConfiguration>>> hooks;

    private Email email;
    private Catalog catalog;

    private ServerConfiguration server;
    private Authentication authentication;

    protected static Logger logger = LoggerFactory.getLogger(Configuration.class);

    public Configuration() {
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        yamlMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    public static Configuration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static Configuration load(InputStream configurationInputStream, String format) throws IOException {
        Configuration configuration;
        ObjectMapper objectMapper;
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
        //TODO : create mandatory fileds check to avoid invalid or incomplete conf
        return configuration;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", logDir='").append(logDir).append('\'');
        sb.append(", openRegister=").append(openRegister);
        sb.append(", userDefaultQuota=").append(userDefaultQuota);
        sb.append(", databasePrefix='").append(databasePrefix).append('\'');
        sb.append(", dataDir='").append(dataDir).append('\'');
        sb.append(", tempJobsDir='").append(tempJobsDir).append('\'');
        sb.append(", toolDir='").append(toolDir).append('\'');
        sb.append(", admin=").append(admin);
        sb.append(", monitor=").append(monitor);
        sb.append(", execution=").append(execution);
        sb.append(", audit=").append(audit);
        sb.append(", hooks=").append(hooks);
        sb.append(", email=").append(email);
        sb.append(", catalog=").append(catalog);
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

    public String getLogDir() {
        return logDir;
    }

    public Configuration setLogDir(String logDir) {
        this.logDir = logDir;
        return this;
    }

    public boolean isOpenRegister() {
        return openRegister;
    }

    public Configuration setOpenRegister(boolean openRegister) {
        this.openRegister = openRegister;
        return this;
    }

    public int getUserDefaultQuota() {
        return userDefaultQuota;
    }

    public Configuration setUserDefaultQuota(int userDefaultQuota) {
        this.userDefaultQuota = userDefaultQuota;
        return this;
    }

    public String getDatabasePrefix() {
        return databasePrefix;
    }

    public Configuration setDatabasePrefix(String databasePrefix) {
        this.databasePrefix = databasePrefix;
        return this;
    }

    public String getDataDir() {
        return dataDir;
    }

    public Configuration setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }

    public String getTempJobsDir() {
        return tempJobsDir;
    }

    public Configuration setTempJobsDir(String tempJobsDir) {
        this.tempJobsDir = tempJobsDir;
        return this;
    }

    public String getToolDir() {
        return toolDir;
    }

    public Configuration setToolDir(String toolDir) {
        this.toolDir = toolDir;
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

    public Execution getExecution() {
        return execution;
    }

    public Configuration setExecution(Execution execution) {
        this.execution = execution;
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

    public Audit getAudit() {
        return audit;
    }

    public Configuration setAudit(Audit audit) {
        this.audit = audit;
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
}
