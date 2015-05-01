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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
//
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
    private List<StorageEngineProperties> storageEngines;

    public StorageConfiguration() {
        storageEngines = new ArrayList<>();
    }

    public StorageConfiguration(String defaultStorageEngine, List<StorageEngineProperties> storageEngines) {
        this.defaultStorageEngine = defaultStorageEngine;
        this.storageEngines = storageEngines;
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    public static StorageConfiguration load(InputStream configurationInputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        return jsonMapper.readValue(configurationInputStream, StorageConfiguration.class);
    }


    public List<StorageEngineProperties> getStorageEngines() {
        return storageEngines;
    }

    public void setStorageEngines(List<StorageEngineProperties> storageEngines) {
        this.storageEngines = storageEngines;
    }

    public String getDefaultStorageEngine() {
        return defaultStorageEngine;
    }

    public void setDefaultStorageEngine(String defaultStorageEngine) {
        this.defaultStorageEngine = defaultStorageEngine;
    }


}
