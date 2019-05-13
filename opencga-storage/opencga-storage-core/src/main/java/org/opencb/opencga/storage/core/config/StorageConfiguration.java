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

package org.opencb.opencga.storage.core.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.opencga.core.config.SearchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 30/04/15.
 */
@JsonIgnoreProperties({"storageEngine", "studyMetadataManager"})
public class StorageConfiguration {

    private String defaultStorageEngineId;
    private String logLevel;
    private String logFile;

    private CellBaseConfiguration cellbase;
    private ServerConfiguration server;
    private CacheConfiguration cache;
    private SearchConfiguration search;
    private SearchConfiguration clinical;
    private IOManagersConfiguration io;

    private BenchmarkConfiguration benchmark;
    private List<StorageEngineConfiguration> storageEngines;

    protected static Logger logger = LoggerFactory.getLogger(StorageConfiguration.class);

    public StorageConfiguration() {
        this("", new ArrayList<>());
    }

    public StorageConfiguration(String defaultStorageEngineId, List<StorageEngineConfiguration> storageEngines) {
        this.defaultStorageEngineId = defaultStorageEngineId;
        this.storageEngines = storageEngines;

        this.cellbase = new CellBaseConfiguration();
        this.server = new ServerConfiguration();
        this.cache = new CacheConfiguration();
        this.search = new SearchConfiguration();
        this.clinical = new SearchConfiguration();
    }


    public static StorageConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static StorageConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        StorageConfiguration storageConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                storageConfiguration = objectMapper.readValue(configurationInputStream, StorageConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                storageConfiguration = objectMapper.readValue(configurationInputStream, StorageConfiguration.class);
                break;
        }

        return storageConfiguration;
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    public StorageEngineConfiguration getStorageEngine() {
        return getStorageEngine(defaultStorageEngineId);
    }

    public StorageEngineConfiguration getStorageEngine(String storageEngine) {
        StorageEngineConfiguration storageEngineConfiguration = null;
        for (StorageEngineConfiguration engine : storageEngines) {
            if (engine.getId().equals(storageEngine)) {
                storageEngineConfiguration = engine;
                break;
            }
        }

        if (storageEngineConfiguration == null && storageEngines.size() > 0) {
            storageEngineConfiguration = storageEngines.get(0);
        }

        return storageEngineConfiguration;
    }

    public void addStorageEngine(InputStream configurationInputStream) throws IOException {
        addStorageEngine(configurationInputStream, "yaml");
    }

    public void addStorageEngine(InputStream configurationInputStream, String format) throws IOException {
        StorageEngineConfiguration storageEngineConfiguration = null;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                storageEngineConfiguration = objectMapper.readValue(configurationInputStream, StorageEngineConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                storageEngineConfiguration = objectMapper.readValue(configurationInputStream, StorageEngineConfiguration.class);
                break;
        }

        if (storageEngineConfiguration != null) {
            this.storageEngines.add(storageEngineConfiguration);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageConfiguration{");
        sb.append("defaultStorageEngineId='").append(defaultStorageEngineId).append('\'');
        sb.append(", logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", cellbase=").append(cellbase);
        sb.append(", server=").append(server);
        sb.append(", cache=").append(cache);
        sb.append(", search=").append(search);
        sb.append(", clinical=").append(clinical);
        sb.append(", benchmark=").append(benchmark);
        sb.append(", storageEngines=").append(storageEngines);
        sb.append('}');
        return sb.toString();
    }

    public String getDefaultStorageEngineId() {
        return defaultStorageEngineId;
    }

    public StorageConfiguration setDefaultStorageEngineId(String defaultStorageEngineId) {
        this.defaultStorageEngineId = defaultStorageEngineId;
        return this;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public StorageConfiguration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getLogFile() {
        return logFile;
    }

    public StorageConfiguration setLogFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

    public CellBaseConfiguration getCellbase() {
        return cellbase;
    }

    public StorageConfiguration setCellbase(CellBaseConfiguration cellbase) {
        this.cellbase = cellbase;
        return this;
    }

    public ServerConfiguration getServer() {
        return server;
    }

    public StorageConfiguration setServer(ServerConfiguration server) {
        this.server = server;
        return this;
    }

    public CacheConfiguration getCache() {
        return cache;
    }

    public StorageConfiguration setCache(CacheConfiguration cache) {
        this.cache = cache;
        return this;
    }

    public SearchConfiguration getSearch() {
        return search;
    }

    public StorageConfiguration setSearch(SearchConfiguration search) {
        this.search = search;
        return this;
    }

    public SearchConfiguration getClinical() {
        return clinical;
    }

    public StorageConfiguration setClinical(SearchConfiguration clinical) {
        this.clinical = clinical;
        return this;
    }

    public IOManagersConfiguration getIo() {
        return io;
    }

    public StorageConfiguration setIo(IOManagersConfiguration io) {
        this.io = io;
        return this;
    }

    public BenchmarkConfiguration getBenchmark() {
        return benchmark;
    }

    public StorageConfiguration setBenchmark(BenchmarkConfiguration benchmark) {
        this.benchmark = benchmark;
        return this;
    }

    public List<StorageEngineConfiguration> getStorageEngines() {
        return storageEngines;
    }

    public StorageConfiguration setStorageEngines(List<StorageEngineConfiguration> storageEngines) {
        this.storageEngines = storageEngines;
        return this;
    }
}
