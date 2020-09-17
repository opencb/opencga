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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.opencb.opencga.server.AbstractStorageServer;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.nio.file.Path;

/**
 * Created by imedina on 02/01/16.
 */
public class GrpcServer extends AbstractStorageServer {

    private Server server;

    public GrpcServer() {
//        this(storageConfiguration.getServer().getGrpc(), storageConfiguration.getDefaultStorageEngineId());
    }

    public GrpcServer(int port, String defaultStorageEngine) {
        super(port, defaultStorageEngine);

        logger = LogManager.getLogger(this.getClass());
    }

    public GrpcServer(Path configDir) {
        super(configDir);
        init();
    }

    @Deprecated
    public GrpcServer(StorageConfiguration storageConfiguration) {
//        super(storageConfiguration.getServer().getGrpc(), storageConfiguration.getDefaultStorageEngineId());
        this.storageConfiguration = storageConfiguration;

        logger = LogManager.getLogger(this.getClass());
    }

    private void init() {
        logger = LogManager.getLogger(this.getClass());
        if (configuration != null) {
            this.port = configuration.getServer().getGrpc().getPort();
        }
    }

    @Override
    public void start() throws Exception {
        server = ServerBuilder.forPort(port)
//                .addService(AdminServiceGrpc.bindService(new AdminGrpcService(catalogConfiguration, storageConfiguration, this)))
//                .addService(VariantServiceGrpc.bindService(new VariantGrpcService(catalogConfiguration, storageConfiguration)))
//                .addService(AlignmentServiceGrpc.bindService(new AlignmentGrpcService(catalogConfiguration, storageConfiguration)))
                .addService(new AdminGrpcService(configuration, storageConfiguration, this))
                .addService(new VariantGrpcService(configuration, storageConfiguration))
                .addService(new AlignmentGrpcService(configuration, storageConfiguration))
                .build()
                .start();
        logger.info("gRPC server started, listening on {}", port);

        // A hook is added in case the JVM is shutting down
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** Shutting down gRPC server since JVM is shutting down");
                GrpcServer.this.stop();
                System.err.println("*** gRPC server shut down");
            }
        });
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    @Override
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
