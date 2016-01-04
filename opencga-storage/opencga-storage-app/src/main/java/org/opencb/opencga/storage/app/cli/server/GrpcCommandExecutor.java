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

package org.opencb.opencga.storage.app.cli.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.storage.server.grpc.GenericServiceModel;
import org.opencb.opencga.storage.server.grpc.GrpcStorageServer;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by imedina on 30/12/15.
 */
public class GrpcCommandExecutor extends CommandExecutor {

    private ServerCliOptionsParser.GrpcCommandOptions grpcCommandOptions;

    public GrpcCommandExecutor(ServerCliOptionsParser.GrpcCommandOptions grpcCommandOptions) {
        this.grpcCommandOptions = grpcCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing gRPC command line");

        String subCommandString = grpcCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "start":
                init(grpcCommandOptions.grpcStartCommandOptions.commonOptions.logLevel,
                        grpcCommandOptions.grpcStartCommandOptions.commonOptions.verbose,
                        grpcCommandOptions.grpcStartCommandOptions.commonOptions.configFile);
                start();
                break;
            case "stop":
                stop();
                break;
            case "status":
                status();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    public void start() throws Exception {
        int port = configuration.getServer().getGrpc();
        if (grpcCommandOptions.grpcStartCommandOptions.port > 0) {
            port = grpcCommandOptions.grpcStartCommandOptions.port;
        }

        // If not --storage-engine is not set then the server will use the default from the storage-configuration.yml
        GrpcStorageServer server = new GrpcStorageServer(port, configuration.getDefaultStorageEngineId());
        server.start();
        server.blockUntilShutdown();
        logger.info("Shutting down Jetty REST server");
    }

    public void stop() throws InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        storageEngine = "mongodb";
        GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                .setStorageEngine(storageEngine)
                .build();


        // Connecting to the server host and port
        String grpcServerHost = "localhost";

        int grpcServerPort = configuration.getServer().getGrpc();
        if (grpcCommandOptions.grpcStopCommandOptions.port > 0) {
            grpcServerPort = grpcCommandOptions.grpcStopCommandOptions.port;
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
