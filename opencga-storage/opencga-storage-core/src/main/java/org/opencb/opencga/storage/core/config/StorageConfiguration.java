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

package org.opencb.opencga.storage.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 30/04/15.
 */
public class StorageConfiguration {

//    ## storage-mongodb plugin configuration
//    #OPENCGA.STORAGE.SEQUENCE.MANAGER    = org.opencb.opencga.storage.mongodb.sequence.MongoDBVariantStorageManager
//    #OPENCGA.STORAGE.ALIGNMENT.MANAGER   = org.opencb.opencga.storage.hbase.alignment.MongoDBAlignmentStorageManager
//    #OPENCGA.STORAGE.VARIANT.MANAGER     = org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager
//    #OPENCGA.STORAGE.CONF                = storage-mongodb.properties
//
//    #Variant
//    OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOSTS  = localhost:27017
//    OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME  = variants
//    #OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER  = biouser
//    #OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS  = biopass

//    OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.BATCH_SIZE         = 100
//    OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.BULK_SIZE          = 100
//    OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.WRITE_THREADS      = 6
//    OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.COMPRESS_GENOTYPES = true
//            #OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.DEFAULT_GENOTYPE   =
//
//    #Alignment
//    OPENCGA.STORAGE.MONGODB.ALIGNMENT.DB.HOSTS = localhost:27017
//    OPENCGA.STORAGE.MONGODB.ALIGNMENT.DB.NAME = opencga-storage
//    #OPENCGA.STORAGE.MONGODB.ALIGNMENT.DB.USER =
//            #OPENCGA.STORAGE.MONGODB.ALIGNMENT.DB.PASS =
//    OPENCGA.STORAGE.ALIGNMENT.TRANSFORM.COVERAGE_CHUNK_SIZE = 10000
//    OPENCGA.STORAGE.ALIGNMENT.TRANSFORM.REGION_SIZE         = 300000
//
//    OPENCGA.STORAGE.VARIANT.TRANSFORM.BATCH_SIZE            = 100
//
//            #OPENCGA.STORAGE.MONGODB.DB.HOST         = localhost
//    #OPENCGA.STORAGE.MONGODB.DB.PORT         = 27017
//            #OPENCGA.STORAGE.MONGODB.DB.USER         =
//            #OPENCGA.STORAGE.MONGODB.DB.PASSWORD     =

    protected static Logger logger = LoggerFactory.getLogger(StorageConfiguration.class);


    private String defaultStorageEngineId;
    private String logLevel;
    private String logFile;
    private String studyMetadataManager;

//    private String include;

    private CellBaseConfiguration cellbase;

    private List<StorageEngineConfiguration> storageEngines;

    public StorageConfiguration() {
        this("", new ArrayList<>());
    }

    public StorageConfiguration(String defaultStorageEngineId, List<StorageEngineConfiguration> storageEngines) {
        this.defaultStorageEngineId = defaultStorageEngineId;
        this.storageEngines = storageEngines;

//        this.include = "conf.d";
        this.cellbase = new CellBaseConfiguration();
    }

    /**
     * This method attempts to find and load the configuration from installation directory,
     * if not exists then loads JAR storage-configuration.yml
     * @throws IOException
     */
    @Deprecated
    public static StorageConfiguration load() throws IOException {
        String appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));
        Path path = Paths.get(appHome + "/conf/storage-configuration.yml");
        if (appHome != null && Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", appHome + "/conf/storage-configuration.yml");
            return StorageConfiguration
                    .load(new FileInputStream(new File(appHome + "/conf/storage-configuration.yml")));
        } else {
            logger.debug("Loading configuration from '{}'",
                    StorageConfiguration.class.getClassLoader()
                            .getResourceAsStream("storage-configuration.yml")
                            .toString());
            return StorageConfiguration
                    .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
        }
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

        if(storageConfiguration != null) {
//            if (storageConfiguration.getDefaultStorageEngineId() != null) {
//                this.defaultStorageEngineId = storageConfiguration.getDefaultStorageEngineId();
//            }
//            if (storageConfiguration.get) {
//                this.include = sto
//                  for(conf.d) ...
//            }
//            for (StorageEngineConfiguration storageEngineConfiguration : storageConfiguration.getStorageEngines()) {
//                this.storageEngines.add(storageEngineConfiguration);
//            }
        }
        return storageConfiguration;
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
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

        if(storageEngineConfiguration == null && storageEngines.size() > 0) {
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

        if(storageEngineConfiguration != null) {
            this.storageEngines.add(storageEngineConfiguration);
        }
    }

    @Override
    public String toString() {
        return "StorageConfiguration{" +
                "defaultStorageEngineId='" + defaultStorageEngineId + '\'' +
                ", logLevel='" + logLevel + '\'' +
//                ", include='" + include + '\'' +
                ", cellbase=" + cellbase +
                ", storageEngines=" + storageEngines +
                '}';
    }


    public String getDefaultStorageEngineId() {
        return defaultStorageEngineId;
    }

    public void setDefaultStorageEngineId(String defaultStorageEngineId) {
        this.defaultStorageEngineId = defaultStorageEngineId;
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

//    public String getInclude() {
//        return include;
//    }
//
//    public void setInclude(String include) {
//        this.include = include;
//    }

    public CellBaseConfiguration getCellbase() {
        return cellbase;
    }

    public void setCellbase(CellBaseConfiguration cellbase) {
        this.cellbase = cellbase;
    }

    public List<StorageEngineConfiguration> getStorageEngines() {
        return storageEngines;
    }

    public void setStorageEngines(List<StorageEngineConfiguration> storageEngines) {
        this.storageEngines = storageEngines;
    }

    public String getStudyMetadataManager() {
        return studyMetadataManager;
    }

    public void setStudyMetadataManager(String studyMetadataManager) {
        this.studyMetadataManager = studyMetadataManager;
    }
}
