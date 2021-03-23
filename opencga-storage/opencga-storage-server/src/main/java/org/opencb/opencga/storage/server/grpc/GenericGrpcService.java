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

package org.opencb.opencga.storage.server.grpc;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.server.grpc.GenericServiceModel.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 16/12/15.
 */
public class GenericGrpcService {

    protected StorageConfiguration storageConfiguration;
    protected String defaultStorageEngine;

    protected static StorageEngineFactory storageEngineFactory;

    private Logger privLogger;
    protected Logger logger;

    public GenericGrpcService(StorageConfiguration storageConfiguration) {
        this(storageConfiguration, storageConfiguration.getVariant().getDefaultEngine());
    }

    public GenericGrpcService(StorageConfiguration storageConfiguration, String defaultStorageEngine) {

        privLogger = LoggerFactory.getLogger("org.opencb.opencga.storage.server.grpc.GenericGrpcService");
        logger = LoggerFactory.getLogger(this.getClass());

        this.storageConfiguration = storageConfiguration;
        this.defaultStorageEngine = defaultStorageEngine;

        // Only one StorageManagerFactory is needed, this acts as a simple Singleton pattern which improves the performance significantly
        if (storageEngineFactory == null) {
            privLogger.debug("Creating the StorageManagerFactory object");
            // TODO: We will need to pass catalog manager once storage starts doing things over catalog
            storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        }
    }

    protected Query createQuery(Request request) {
        Query query = new Query();
        for (String key : request.getQuery().keySet()) {
            if (request.getQuery().get(key) != null) {
                query.put(key, request.getQuery().get(key));
            }
        }
        return query;
    }

    protected QueryOptions createQueryOptions(Request request) {
        QueryOptions queryOptions = new QueryOptions();
        for (String key : request.getOptions().keySet()) {
            if (request.getOptions().get(key) != null) {
                queryOptions.put(key, request.getOptions().get(key));
            }
        }
        return queryOptions;
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
