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

package org.opencb.opencga.storage.server.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by imedina on 16/12/15.
 */
public class GenericGrpcServer {

    private Server server;
    private int port = 9091;

    private static StorageConfiguration storageConfiguration;
//    protected static DBAdaptorFactory dbAdaptorFactory;
    protected static StorageManagerFactory storageManagerFactory;

    protected static Logger logger; // = Logger.getLogger(GeneServer.class.getName());

    public GenericGrpcServer() {
        this.port = storageConfiguration.getServer().getGrpc();
    }

    public GenericGrpcServer(int port) {
        this.port = port;
    }

    static {
        logger = LoggerFactory.getLogger("org.opencb.opencga.storage.server.grpc.GenericGrpcServer");
        logger.info("Static block, creating StorageManagerFactory");
        try {
            if (System.getenv("OPENCGA_HOME") != null) {
                logger.info("Loading configuration from '{}'", System.getenv("OPENCGA_HOME") + "/conf/storage-configuration.yml");
                storageConfiguration = StorageConfiguration
                        .load(new FileInputStream(new File(System.getenv("OPENCGA_HOME") + "/conf/storage-configuration.yml")));
            } else {
                logger.info("Loading configuration from '{}'",
                        StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml").toString());
                storageConfiguration = StorageConfiguration
                        .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
            }

            // If Configuration has been loaded we can create the DBAdaptorFactory
//            dbAdaptorFactory = new org.opencb.cellbase.mongodb.impl.MongoDBAdaptorFactory(storageConfiguration);
            storageManagerFactory = new StorageManagerFactory(storageConfiguration);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws Exception {
        server = ServerBuilder.forPort(port)
//                .addService(GeneServiceGrpc.bindService(new GeneGrpcServer()))
                .addService(org.opencb.opencga.storage.server.grpc.VariantServiceGrpc.bindService(new VariantGrpcServer()))
                .build()
                .start();
        logger.info("Server started, listening on {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                GenericGrpcServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


    protected Query createQuery(org.opencb.opencga.storage.server.grpc.GenericServiceModel.Request request) {
        Query query = new Query();
        for (String key : request.getQuery().keySet()) {
            if (request.getQuery().get(key) != null) {
                query.put(key, request.getQuery().get(key));
            }
        }
        return query;
    }

    protected QueryOptions createQueryOptions(org.opencb.opencga.storage.server.grpc.GenericServiceModel.Request request) {
        QueryOptions queryOptions = new QueryOptions();
        for (String key : request.getOptions().keySet()) {
            if (request.getOptions().get(key) != null) {
                queryOptions.put(key, request.getOptions().get(key));
            }
        }
        return queryOptions;
    }


//    public static void main(String[] args) throws Exception {
//        final GenericGrpcServer server = new GenericGrpcServer();
//        server.start();
//        server.blockUntilShutdown();
//    }

}
