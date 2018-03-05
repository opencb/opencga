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

package org.opencb.opencga.server.grpc;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 16/12/15.
 */
public class GenericGrpcService {

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected String defaultStorageEngine;

    protected CatalogManager catalogManager;
//    protected VariantStorageManager variantStorageManager;
    protected static StorageEngineFactory storageEngineFactory;

    private Logger privLogger;
    protected Logger logger;

    public GenericGrpcService(Configuration configuration, StorageConfiguration storageConfiguration) {
        this(configuration, storageConfiguration, storageConfiguration.getDefaultStorageEngineId());
    }

    public GenericGrpcService(Configuration configuration, StorageConfiguration storageConfiguration, String defaultStorageEngine) {
        privLogger = LoggerFactory.getLogger(this.getClass().toString());
        logger = LoggerFactory.getLogger(this.getClass().toString());

        this.configuration = configuration;
        this.storageConfiguration = storageConfiguration;
        this.defaultStorageEngine = defaultStorageEngine;

        try {
            catalogManager = new CatalogManager(configuration);
        } catch (CatalogException e) {
            throw new IllegalStateException("Error initializating Catalog: ", e);
        }

        // Only one StorageManagerFactory is needed, this acts as a simple Singleton pattern which improves the performance significantly
        if (storageEngineFactory == null) {
            privLogger.debug("Creating the StorageManagerFactory object");
            storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        }

//        variantStorageManager = new VariantStorageManager(catalogManager, storageEngineFactory);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenericGrpcServer{");
        sb.append("storageConfiguration=").append(storageConfiguration);
        sb.append(", defaultStorageEngine='").append(defaultStorageEngine).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public void setStorageConfiguration(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
    }

    public String getDefaultStorageEngine() {
        return defaultStorageEngine;
    }

    public void setDefaultStorageEngine(String defaultStorageEngine) {
        this.defaultStorageEngine = defaultStorageEngine;
    }

}
