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

package org.opencb.opencga.server.grpc;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.server.grpc.GenericServiceModel.Request;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 16/12/15.
 */
public class GenericGrpcService {

    protected CatalogConfiguration catalogConfiguration;
    protected StorageConfiguration storageConfiguration;
    protected String defaultStorageEngine;

    protected CatalogManager catalogManager;
    protected static StorageManagerFactory storageManagerFactory;

//    protected AuthManager authManager;
//    protected Set<String> authorizedHosts;

    private Logger privLogger;
    protected Logger logger;

    public GenericGrpcService(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration) {
        this(catalogConfiguration, storageConfiguration, storageConfiguration.getDefaultStorageEngineId());
    }

    public GenericGrpcService(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration, String defaultStorageEngine) {

        privLogger = LoggerFactory.getLogger(this.getClass().toString());
        logger = LoggerFactory.getLogger(this.getClass().toString());

        this.catalogConfiguration = catalogConfiguration;
        this.storageConfiguration = storageConfiguration;
        this.defaultStorageEngine = defaultStorageEngine;


        try {
            catalogManager = new CatalogManager(catalogConfiguration);
        } catch (CatalogException e) {
            e.printStackTrace();
        }

        // Only one StorageManagerFactory is needed, this acts as a simple Singleton pattern which improves the performance significantly
        if (storageManagerFactory == null) {
            privLogger.debug("Creating the StorageManagerFactory object");
            storageManagerFactory = new StorageManagerFactory(storageConfiguration);
        }

//        if (authorizedHosts == null) {
//            privLogger.debug("Creating the authorizedHost HashSet");
//            authorizedHosts = new HashSet<>(storageConfiguration.getServer().getAuthorizedHosts());
//        }

//        try {
//            if (StringUtils.isNotEmpty(storageConfiguration.getServer().getAuthManager())) {
//                privLogger.debug("Loading AuthManager in {} from {}", this.getClass(), storageConfiguration.getServer().getAuthManager());
//                authManager = (AuthManager) Class.forName(storageConfiguration.getServer().getAuthManager()).newInstance();
//            } else {
//                privLogger.debug("Loading DefaultAuthManager in {} from {}", this.getClass(), DefaultAuthManager.class);
//                authManager = new DefaultAuthManager();
//            }
//        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
    }

//    protected void checkAuthorizedHosts(Query query, String ip) throws NotAuthorizedHostException, NotAuthorizedUserException {
//        if (authorizedHosts.contains("0.0.0.0") || authorizedHosts.contains("*") || authorizedHosts.contains(ip)) {
//            authManager.checkPermission(query, "");
//        } else {
//            throw new NotAuthorizedHostException("No queries are allowed from " + ip);
//        }
//    }

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
