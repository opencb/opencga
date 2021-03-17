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

package org.opencb.opencga.storage.app.cli.server.executors;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.storage.app.cli.server.options.ServerCommandOptions;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.storage.server.grpc.GenericServiceModel;
import org.opencb.opencga.storage.server.grpc.GrpcStorageServer;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by imedina on 30/12/15.
 */
public class GrpcCommandExecutor {// extends CommandExecutor {

    ServerCommandOptions.GrpcServerCommandOptions grpcServerCommandOptions;
    StorageConfiguration configuration;
    Logger logger;

    public GrpcCommandExecutor(ServerCommandOptions.GrpcServerCommandOptions grpcServerCommandOptions,
                               StorageConfiguration configuration, Logger logger) {
        this.grpcServerCommandOptions = grpcServerCommandOptions;
        this.configuration = configuration;
        this.logger = logger;
    }

//    private AdminCliOptionsParser.GrpcCommandOptions grpcCommandOptions;
//
//    public GrpcCommandExecutor(AdminCliOptionsParser.GrpcCommandOptions grpcCommandOptions) {
//        super(grpcCommandOptions.commonOptions);
//        this.grpcCommandOptions = grpcCommandOptions;
//    }
//
//
//    @Override
//    public void execute() throws Exception {
//        logger.debug("Executing gRPC command line");
//
//        String subCommandString = grpcCommandOptions.getParsedSubCommand();
//        switch (subCommandString) {
//            case "start":
//                init(grpcCommandOptions.grpcStartCommandOptions.commonOptions.logLevel,
//                        grpcCommandOptions.grpcStartCommandOptions.commonOptions.verbose,
//                        grpcCommandOptions.grpcStartCommandOptions.commonOptions.configFile,
//                        grpcCommandOptions.grpcStartCommandOptions.commonOptions.storageEngine);
//                start();
//                break;
//            case "stop":
//                stop();
//                break;
//            case "status":
//                status();
//                break;
//            default:
//                logger.error("Subcommand not valid");
//                break;
//        }
//    }

    public void start() throws Exception {
//        int port = configuration.getServer().getGrpc();
//        if (grpcCommandOptions.grpcStartCommandOptions.port > 0) {
//            port = grpcCommandOptions.grpcStartCommandOptions.port;
//        }
//
//        String storageEngine = configuration.getDefaultStorageEngineId();
//        if (StringUtils.isNotEmpty(grpcCommandOptions.grpcStartCommandOptions.commonOptions.storageEngine)) {
//            storageEngine = grpcCommandOptions.grpcStartCommandOptions.commonOptions.storageEngine;
//        }


        // If not --storage-engine is not set then the server will use the default from the storage-configuration.yml
        StorageConfiguration storageConfiguration = configuration;
        if (StringUtils.isNotEmpty(grpcServerCommandOptions.commonOptions.configFile)) {
            Path path = Paths.get(grpcServerCommandOptions.commonOptions.configFile);
            if (Files.exists(path)) {
                storageConfiguration = StorageConfiguration.load(Files.newInputStream(path));
            }
        }

        // Setting CLI params in the StorageConfiguration
        if (grpcServerCommandOptions.port > 0) {
            storageConfiguration.getServer().getGrpc().setPort(grpcServerCommandOptions.port);
        }

        if (StringUtils.isNotEmpty(grpcServerCommandOptions.commonOptions.storageEngine)) {
            storageConfiguration.getVariant().setDefaultEngine(grpcServerCommandOptions.commonOptions.storageEngine);
        }

        // Server crated and started
        GrpcStorageServer server = new GrpcStorageServer(storageConfiguration);
        server.start();
        server.blockUntilShutdown();
        logger.info("Shutting down gRPC server");
    }

    public void stop() throws InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        String storageEngine = "mongodb";
        GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                .setStorageEngine(storageEngine)
                .build();


        // Connecting to the server host and port
        String grpcServerHost = "localhost";

        int grpcServerPort = configuration.getServer().getGrpc().getPort();
        if (grpcServerCommandOptions.port > 0) {
            grpcServerPort = grpcServerCommandOptions.port;
        }
        logger.debug("Stopping gRPC server at '{}:{}'", grpcServerHost, grpcServerPort);

        // We create the gRPC channel to the specified server host and port
        ManagedChannel channel = ManagedChannelBuilder.forAddress(grpcServerHost, grpcServerPort)
                .usePlaintext(true)
                .build();


        // We use a blocking stub to execute the query to gRPC
        AdminServiceGrpc.AdminServiceBlockingStub adminServiceBlockingStub = AdminServiceGrpc.newBlockingStub(channel);
        GenericServiceModel.MapResponse stop = adminServiceBlockingStub.stop(request);
        Map<String, String> values = stop.getValues();
        System.out.println(values);
        channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
    }

    public void status() {

    }

}
