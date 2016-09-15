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

package org.opencb.opencga.catalog.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created by imedina on 16/03/16.
 */
public class CatalogConfiguration {

    private String logLevel;
    private String logFile;

    private boolean openRegister;
    private int userDefaultDiskQuota;

    private String databasePrefix;
    private String dataDir;
    private String tempJobsDir;
    private String toolsDir;

    private Admin admin;
    private List<AuthenticationOrigin> authenticationOrigins;
    private Monitor monitor;
    private Execution execution;
    private Audit audit;

    private List<StudyAclEntry> acl;

    private EmailServer emailServer;
    private DatabaseCredentials database;
//    private Policies policies;


    protected static Logger logger = LoggerFactory.getLogger(CatalogConfiguration.class);

    public CatalogConfiguration() {
    }

//    public CatalogConfiguration(String defaultStorageEngineId, List<StorageEngineConfiguration> storageEngines) {
//        this.defaultStorageEngineId = defaultStorageEngineId;
//        this.storageEngines = storageEngines;
//
//        this.cellbase = new CellBaseConfiguration();
//        this.server = new QueryServerConfiguration();
//    }


    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        yamlMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    public static CatalogConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static CatalogConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        CatalogConfiguration catalogConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                catalogConfiguration = objectMapper.readValue(configurationInputStream, CatalogConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                catalogConfiguration = objectMapper.readValue(configurationInputStream, CatalogConfiguration.class);
                break;
        }
        return catalogConfiguration;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CatalogConfiguration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", openRegister=").append(openRegister);
        sb.append(", userDefaultDiskQuota=").append(userDefaultDiskQuota);
        sb.append(", dataDir='").append(dataDir).append('\'');
        sb.append(", tempJobsDir='").append(tempJobsDir).append('\'');
        sb.append(", toolsDir='").append(toolsDir).append('\'');
        sb.append(", admin=").append(admin);
        sb.append(", authenticationOrigins=").append(authenticationOrigins);
        sb.append(", monitor=").append(monitor);
        sb.append(", execution=").append(execution);
        sb.append(", audit=").append(audit);
        sb.append(", acl=").append(acl);
        sb.append(", emailServer=").append(emailServer);
        sb.append(", database=").append(database);
        sb.append('}');
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public CatalogConfiguration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getLogFile() {
        return logFile;
    }

    public CatalogConfiguration setLogFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

    public boolean isOpenRegister() {
        return openRegister;
    }

    public CatalogConfiguration setOpenRegister(boolean openRegister) {
        this.openRegister = openRegister;
        return this;
    }

    public int getUserDefaultDiskQuota() {
        return userDefaultDiskQuota;
    }

    public CatalogConfiguration setUserDefaultDiskQuota(int userDefaultDiskQuota) {
        this.userDefaultDiskQuota = userDefaultDiskQuota;
        return this;
    }

    public String getDatabasePrefix() {
        return databasePrefix;
    }

    public CatalogConfiguration setDatabasePrefix(String databasePrefix) {
        this.databasePrefix = databasePrefix;
        return this;
    }

    public String getDataDir() {
        return dataDir;
    }

    public CatalogConfiguration setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }

    public String getTempJobsDir() {
        return tempJobsDir;
    }

    public CatalogConfiguration setTempJobsDir(String tempJobsDir) {
        this.tempJobsDir = tempJobsDir;
        return this;
    }

    public String getToolsDir() {
        return toolsDir;
    }

    public CatalogConfiguration setToolsDir(String toolsDir) {
        this.toolsDir = toolsDir;
        return this;
    }

    public Admin getAdmin() {
        return admin;
    }

    public CatalogConfiguration setAdmin(Admin admin) {
        this.admin = admin;
        return this;
    }

    public List<AuthenticationOrigin> getAuthenticationOrigins() {
        return authenticationOrigins;
    }

    public CatalogConfiguration setAuthenticationOrigins(List<AuthenticationOrigin> authenticationOrigins) {
        this.authenticationOrigins = authenticationOrigins;
        return this;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public CatalogConfiguration setMonitor(Monitor monitor) {
        this.monitor = monitor;
        return this;
    }

    public Execution getExecution() {
        return execution;
    }

    public CatalogConfiguration setExecution(Execution execution) {
        this.execution = execution;
        return this;
    }

    public EmailServer getEmailServer() {
        return emailServer;
    }

    public CatalogConfiguration setEmailServer(EmailServer emailServer) {
        this.emailServer = emailServer;
        return this;
    }

    public DatabaseCredentials getDatabase() {
        return database;
    }

    public CatalogConfiguration setDatabase(DatabaseCredentials database) {
        this.database = database;
        return this;
    }

    public Audit getAudit() {
        return audit;
    }

    public CatalogConfiguration setAudit(Audit audit) {
        this.audit = audit;
        return this;
    }

    public List<StudyAclEntry> getAcl() {
        return acl;
    }

    public CatalogConfiguration setAcl(List<StudyAclEntry> acl) {
        this.acl = acl;
        return this;
    }
}
