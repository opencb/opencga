/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.server.grpc.GenericServiceModel.Request;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
/**
 * Created by imedina on 16/12/15.
 */
public class GenericGrpcService {

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected String defaultStorageEngine;

    protected CatalogManager catalogManager;
    protected VariantStorageManager variantStorageManager;
    protected static StorageEngineFactory storageEngineFactory;

//    protected AuthManager authManager;
//    protected Set<String> authorizedHosts;

    private Logger privLogger;
    protected Logger logger;

    public GenericGrpcService(Configuration configuration, StorageConfiguration storageConfiguration) {
        this(configuration, storageConfiguration, storageConfiguration.getVariant().getDefaultEngine());
    }

    public GenericGrpcService(Configuration configuration, StorageConfiguration storageConfiguration, String defaultStorageEngine) {

        privLogger = LogManager.getLogger(this.getClass().toString());
        logger = LogManager.getLogger(this.getClass().toString());

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

        variantStorageManager = new VariantStorageManager(catalogManager, storageEngineFactory);


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
        for (String key : request.getQueryMap().keySet()) {
            if (request.getQueryMap().get(key) != null) {
                query.put(key, request.getQueryMap().get(key));
            }
        }
        return query;
    }

    protected QueryOptions createQueryOptions(Request request) {
        QueryOptions queryOptions = new QueryOptions();
        for (String key : request.getOptionsMap().keySet()) {
            if (request.getOptionsMap().get(key) != null) {
                queryOptions.put(key, request.getOptionsMap().get(key));
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
