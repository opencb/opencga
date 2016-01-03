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
import org.opencb.opencga.storage.server.common.AbstractStorageServer;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 02/01/16.
 */
public class GrpcStorageServer extends AbstractStorageServer {

    private Server server;

    public GrpcStorageServer() {
        this(storageConfiguration.getServer().getGrpc(), storageConfiguration.getDefaultStorageEngineId());
    }

    public GrpcStorageServer(int port) {
        this(port, storageConfiguration.getDefaultStorageEngineId());
    }

    public GrpcStorageServer(int port, String defaultStorageEngine) {
        super(port, defaultStorageEngine);

        logger = LoggerFactory.getLogger("org.opencb.opencga.storage.server.grpc.StorageGrpcServer");
    }

    @Override
    public void start() throws Exception {
        server = ServerBuilder.forPort(port)
//                .addService(GeneServiceGrpc.bindService(new GeneGrpcServer()))
                .addService(AdminServiceGrpc.bindService(new AdminGrpcService(storageConfiguration, this)))
                .addService(VariantServiceGrpc.bindService(new VariantGrpcService(storageConfiguration, defaultStorageEngine)))
                .build()
                .start();
        logger.info("gRPC server started, listening on {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                GrpcStorageServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    @Override
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

}
