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
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by imedina on 16/12/15.
 */
public class GrpcServiceUtils {

    protected StorageConfiguration storageConfiguration;
    protected String defaultStorageEngine;

    protected static StorageEngineFactory storageEngineFactory;
    private Logger logger;

    @Deprecated
    public GrpcServiceUtils(StorageConfiguration storageConfiguration) {
        this(storageConfiguration, storageConfiguration.getDefaultStorageEngineId());
    }

    @Deprecated
    public GrpcServiceUtils(StorageConfiguration storageConfiguration, String defaultStorageEngine) {
        logger = LoggerFactory.getLogger(this.getClass());

        this.storageConfiguration = storageConfiguration;
        this.defaultStorageEngine = defaultStorageEngine;

        // Only one StorageManagerFactory is needed, this acts as a simple Singleton pattern which improves the performance significantly
        if (storageEngineFactory == null) {
            logger.debug("Creating the StorageManagerFactory object");
            // TODO: We will need to pass catalog manager once storage starts doing things over catalog
            storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        }
    }

    public static Query createQuery(Map<String, String> request) {
        Query query = new Query();
        for (String key : request.keySet()) {
            query.putIfNotNull(key, request.get(key));
        }
        return query;
    }

    public static QueryOptions createQueryOptions(Map<String, String> request) {
        QueryOptions queryOptions = new QueryOptions();
        for (String key : request.keySet()) {
            queryOptions.putIfNotNull(key, request.get(key));
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
