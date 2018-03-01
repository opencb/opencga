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

package org.opencb.opencga.storage.app.cli.executors;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.options.ServerStorageCommandOptions;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.storage.server.grpc.GenericServiceModel;
import org.opencb.opencga.storage.server.grpc.GrpcStorageServer;
import org.opencb.opencga.storage.server.rest.RestStorageServer;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by joaquin on 2/2/17.
 */
public class ServerStorageCommandExecutor extends CommandExecutor {

    private ServerStorageCommandOptions serverStorageCommandOptions;

    public ServerStorageCommandExecutor(ServerStorageCommandOptions serverStorageCommandOptions) {
        super(serverStorageCommandOptions.commonCommandOptions);
        this.serverStorageCommandOptions = serverStorageCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        String subCommandString = getParsedSubCommand(serverStorageCommandOptions.jCommander);
        switch (subCommandString) {
            case "rest":
                rest();
                break;
            case "grpc":
                grpc();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    /**
     * rest support: start, stop, status...
     */
    void rest() throws Exception {
        if (serverStorageCommandOptions.restServerCommandOptions.start) {
            restStart(serverStorageCommandOptions.restServerCommandOptions);
        } else if (serverStorageCommandOptions.restServerCommandOptions.stop) {
            restStop(serverStorageCommandOptions.restServerCommandOptions);
        } else {
            logger.error(usage("REST"));
        }
    }

    /**
     * gRPC support: start, stop, status...
     */
    void grpc() throws Exception {
        if (serverStorageCommandOptions.grpcServerCommandOptions.start) {
            grpcStart(serverStorageCommandOptions.grpcServerCommandOptions);
        } else if (serverStorageCommandOptions.grpcServerCommandOptions.stop) {
            grpcStop(serverStorageCommandOptions.grpcServerCommandOptions);
        } else {
            logger.error(usage("gRPC"));
        }
    }


    private void restStart(ServerStorageCommandOptions.RestServerCommandOptions restServerCommandOptions) throws Exception {
        if (StringUtils.isNotEmpty(restServerCommandOptions.commonOptions.configFile)) {
            Path path = Paths.get(restServerCommandOptions.commonOptions.configFile);
            if (Files.exists(path)) {
                storageConfiguration = StorageConfiguration.load(Files.newInputStream(path));
            }
        }

        // Setting CLI params in the StorageConfiguration
        if (restServerCommandOptions.port > 0) {
            storageConfiguration.getServer().setRest(restServerCommandOptions.port);
        }

        if (StringUtils.isNotEmpty(restServerCommandOptions.commonOptions.storageEngine)) {
            storageConfiguration.setDefaultStorageEngineId(restServerCommandOptions.commonOptions.storageEngine);
        }

        if (StringUtils.isNotEmpty(restServerCommandOptions.authManager)) {
            storageConfiguration.getServer().setAuthManager(restServerCommandOptions.authManager);
        }

        // Server crated and started
        RestStorageServer server = new RestStorageServer(storageConfiguration);
        server.start();
        server.blockUntilShutdown();
        logger.info("Shutting down OpenCGA Storage REST server");
    }

    private void restStop(ServerStorageCommandOptions.RestServerCommandOptions restServerCommandOptions) {
        int port = storageConfiguration.getServer().getRest();
        if (restServerCommandOptions.port > 0) {
            port = restServerCommandOptions.port;
        }

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target("http://localhost:" + port)
                .path("opencga")
                .path("webservices")
                .path("rest")
                .path("admin")
                .path("stop");
        Response response = target.request().get();
        logger.info(response.toString());
    }

    private void grpcStart(ServerStorageCommandOptions.GrpcServerCommandOptions grpcServerCommandOptions) throws Exception {
        if (StringUtils.isNotEmpty(grpcServerCommandOptions.commonOptions.configFile)) {
            Path path = Paths.get(grpcServerCommandOptions.commonOptions.configFile);
            if (Files.exists(path)) {
                storageConfiguration = StorageConfiguration.load(Files.newInputStream(path));
            }
        }

        // Setting CLI params in the StorageConfiguration
        if (grpcServerCommandOptions.port > 0) {
            storageConfiguration.getServer().setGrpc(grpcServerCommandOptions.port);
        }

        if (StringUtils.isNotEmpty(grpcServerCommandOptions.commonOptions.storageEngine)) {
            storageConfiguration.setDefaultStorageEngineId(grpcServerCommandOptions.commonOptions.storageEngine);
        }

        if (StringUtils.isNotEmpty(grpcServerCommandOptions.authManager)) {
            storageConfiguration.getServer().setAuthManager(grpcServerCommandOptions.authManager);
        }

        // Server crated and started
        GrpcStorageServer server = new GrpcStorageServer(storageConfiguration);
        server.start();
        server.blockUntilShutdown();
        logger.info("Shutting down gRPC server");
    }

    private void grpcStop(ServerStorageCommandOptions.GrpcServerCommandOptions grpcServerCommandOptions) throws InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String storageEngine = "mongodb";
        GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                .setStorageEngine(storageEngine)
                .build();

        // Connecting to the server host and port
        String grpcServerHost = "localhost";

        int grpcServerPort = storageConfiguration.getServer().getGrpc();
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

    /**
     * Error message when no action was specified.
     *
     * @param type  Type of server: REST, GRPC
     * @return      Error message
     */
    private String usage(String type) {
        StringBuilder sb = new StringBuilder();
        sb.append("None action for the ").append(type).append(" server.\n");
        sb.append("\tTo start the server, use --start.\n");
        sb.append("\tTo stop the server, use --stop.\n");
        sb.append("For additional parameters, use -h or --help.\n");
        return sb.toString();
    }
}
