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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.server.OpenCGAHealthCheckMonitor;
import org.opencb.opencga.server.OpenCGAServerUtils;
import org.opencb.opencga.server.grpc.GenericServiceModel.Request;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.core.common.JacksonUtils.getExternalOpencgaObjectMapper;

/**
 * Created by imedina on 16/12/15.
 */
public class GenericGrpcService {

    private final ObjectWriter jsonObjectWriter;
    private final Configuration configuration;
    private final StorageConfiguration storageConfiguration;

    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;
    private OpenCGAHealthCheckMonitor healthCheckMonitor;
    private StorageEngineFactory storageEngineFactory;

//    protected AuthManager authManager;
//    protected Set<String> authorizedHosts;

    protected final Logger logger;

    public GenericGrpcService(Path opencgaHome, Configuration configuration, StorageConfiguration storageConfiguration)
            throws CatalogException {

        logger = LoggerFactory.getLogger(GenericGrpcService.class);

        ObjectMapper jsonObjectMapper = getExternalOpencgaObjectMapper();
        jsonObjectWriter = jsonObjectMapper.writer();

        this.configuration = configuration;
        this.storageConfiguration = storageConfiguration;

        logger.info("========================================================================");
        logger.info("| Starting OpenCGA GRPC server, initializing GenericGrpcService");
        logger.info("| This message must appear only once.");


        // Check and execute the init methods
        java.nio.file.Path configDirPath = opencgaHome.resolve("conf");
        logger.info("|  * OpenCGA home: '{}'", opencgaHome);
        logger.info("|  * Configuration folder: '{}'", configDirPath);
//        loadOpenCGAConfiguration(configDirPath);
        OpenCGAServerUtils.initLogger(logger, configuration, configDirPath);
        initOpenCGAObjects();


        logger.info("| OpenCGA GRPC successfully started!");
        logger.info("| - Version {}", GitRepositoryState.getInstance().getBuildVersion());
        logger.info("| - Git version: {} {}", GitRepositoryState.getInstance().getBranch(), GitRepositoryState.getInstance().getCommitId());
        logger.info("========================================================================\n");
    }

    private void initOpenCGAObjects() throws CatalogException {

        catalogManager = new CatalogManager(configuration);
        storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        variantStorageManager = new VariantStorageManager(catalogManager, storageEngineFactory);
        healthCheckMonitor = new OpenCGAHealthCheckMonitor(configuration, catalogManager, storageEngineFactory, variantStorageManager);
        healthCheckMonitor.asyncUpdate();

    }

//    protected void checkAuthorizedHosts(Query query, String ip) throws NotAuthorizedHostException, NotAuthorizedUserException {
//        if (authorizedHosts.contains("0.0.0.0") || authorizedHosts.contains("*") || authorizedHosts.contains(ip)) {
//            authManager.checkPermission(query, "");
//        } else {
//            throw new NotAuthorizedHostException("No queries are allowed from " + ip);
//        }
//    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenericGrpcServer{");
        sb.append("storageConfiguration=").append(storageConfiguration);
        sb.append('}');
        return sb.toString();
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public VariantStorageManager getVariantStorageManager() {
        return variantStorageManager;
    }

    public StorageEngineFactory getStorageEngineFactory() {
        return storageEngineFactory;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public OpenCGAHealthCheckMonitor getHealthCheckMonitor() {
        return healthCheckMonitor;
    }

    @FunctionalInterface
    public interface RequestRunner {

        int run(Query query, QueryOptions options) throws Exception;

    }

    @FunctionalInterface
    public interface RequestRunnerReturnless {

        void run(Query query, QueryOptions options) throws Exception;

    }

    public void run(MethodDescriptor<Request, ?> method, Request request, StreamObserver<?> streamObserver,
                    RequestRunnerReturnless requestRunner) {
        run(method, request, streamObserver, (query, queryOptions) -> {
            requestRunner.run(query, queryOptions);
            return 1;
        });
    }
    public void run(MethodDescriptor<Request, ?> method, Request request, StreamObserver<?> streamObserver,
                    RequestRunner requestRunner) {
        StopWatch stopWatch = StopWatch.createStarted();
//        Query query = createQuery(request);
//        QueryOptions queryOptions = createQueryOptions(request);
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();
        OpenCGAServerUtils.parseParams(request.getQueryMap(), query, queryOptions);

        String requestDescription;
        try {
            requestDescription = method.getFullMethodName() + " : q: " + jsonObjectWriter.writeValueAsString(query)
                    + " qo: " + jsonObjectWriter.writeValueAsString(queryOptions);
            logger.info(requestDescription);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        Exception e = null;
        int numResults = -1;
        try {
            numResults = requestRunner.run(query, queryOptions);
            streamObserver.onCompleted();
        } catch (Exception ex) {
            e = ex;
            logger.error("Catch error: " + e.getMessage(), e);
            streamObserver.onError(ex);
        } finally {
            stopWatch.stop();
            logEnd(e, stopWatch, numResults, requestDescription);
        }

    }

    private void logEnd(Exception e, StopWatch stopWatch, int numResults, String requestDescription) {
        StringBuilder sb = new StringBuilder();
        boolean ok;
        if (e == null) {
            sb.append("OK");
            ok = true;
        } else {
            sb.append("ERROR");
            ok = false;
        }
        sb.append(", ").append(stopWatch.getTime(TimeUnit.MILLISECONDS)).append("ms");
        if (numResults >= 0) {
            sb.append(", ").append(numResults).append(" results");
        }
        sb.append(", ").append(requestDescription);
        if (ok) {
            logger.info(sb.toString());
        } else {
            logger.error(sb.toString());
        }
    }
}
