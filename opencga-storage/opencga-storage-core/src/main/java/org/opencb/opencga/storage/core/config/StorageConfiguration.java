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

import java.io.*;
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



    private String defaultStorageEngine;
    private String include;
    private CellBaseConfiguration cellbase;
    private List<StorageEngineConfiguration> engines;

    public StorageConfiguration() {
        this("", new ArrayList<>());
    }

    public StorageConfiguration(String defaultStorageEngine, List<StorageEngineConfiguration> engines) {
        this.defaultStorageEngine = defaultStorageEngine;
        this.engines = engines;

        this.include = "conf.d";
        this.cellbase = new CellBaseConfiguration();
    }

    public void load(InputStream configurationInputStream) throws IOException {
        load(configurationInputStream, "yaml");
    }



    public void load(InputStream configurationInputStream, String format) throws IOException {
        StorageConfiguration storageConfiguration = null;
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
            if (storageConfiguration.getDefaultStorageEngine() != null) {
                this.defaultStorageEngine = storageConfiguration.getDefaultStorageEngine();
            }
//            if (storageConfiguration.get) {
//                this.include = sto
//                  for(conf.d) ...
//            }
            for (StorageEngineConfiguration storageEngineConfiguration : storageConfiguration.getEngines()) {
                this.engines.add(storageEngineConfiguration);
            }
        }
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
            this.engines.add(storageEngineConfiguration);
        }
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    public List<StorageEngineConfiguration> getEngines() {
        return engines;
    }


    public void setEngines(List<StorageEngineConfiguration> engines) {
        this.engines = engines;
    }

    public String getDefaultStorageEngine() {
        return defaultStorageEngine;
    }

    public void setDefaultStorageEngine(String defaultStorageEngine) {
        this.defaultStorageEngine = defaultStorageEngine;
    }

    public String getInclude() {
        return include;
    }

    public void setInclude(String include) {
        this.include = include;
    }

    public CellBaseConfiguration getCellbase() {
        return cellbase;
    }

    public void setCellbase(CellBaseConfiguration cellbase) {
        this.cellbase = cellbase;
    }

}
