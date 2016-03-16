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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by imedina on 16/03/16.
 */
public class CatalogConfiguration {

    private String logLevel;
    private String logFile;

    private String dataDir;
    private String tempJobsDir;

    private String user;
    private String password;

    private EmailServer emailServer;
    private DatabaseCredentials database;

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
        sb.append(", dataDir='").append(dataDir).append('\'');
        sb.append(", tempJobsDir='").append(tempJobsDir).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", emailServer=").append(emailServer);
        sb.append(", database=").append(database);
        sb.append('}');
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getTempJobsDir() {
        return tempJobsDir;
    }

    public void setTempJobsDir(String tempJobsDir) {
        this.tempJobsDir = tempJobsDir;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public EmailServer getEmailServer() {
        return emailServer;
    }

    public void setEmailServer(EmailServer emailServer) {
        this.emailServer = emailServer;
    }

    public DatabaseCredentials getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseCredentials database) {
        this.database = database;
    }
}
