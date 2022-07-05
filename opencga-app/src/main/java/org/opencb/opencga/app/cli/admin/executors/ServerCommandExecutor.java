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

package org.opencb.opencga.app.cli.admin.executors;


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.opencb.biodata.models.common.protobuf.service.ServiceTypesModel;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.main.impl.CommandExecutorImpl;
import org.opencb.opencga.server.RestServer;
import org.opencb.opencga.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.server.grpc.GrpcServer;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.nio.file.Paths;

/**
 * Created by imedina on 02/03/15.
 */
public class ServerCommandExecutor extends CommandExecutorImpl {

    private final AdminCliOptionsParser.ServerCommandOptions serverCommandOptions;

    public ServerCommandExecutor(AdminCliOptionsParser.ServerCommandOptions serverCommandOptions) {
        super(serverCommandOptions.commonOptions.commonOptions, true);
        this.serverCommandOptions = serverCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = serverCommandOptions.getParsedSubCommand();
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

    private void rest() throws Exception {
        int port = (serverCommandOptions.restServerCommandOptions.port == 0)
                ? configuration.getServer().getRest().getPort()
                : serverCommandOptions.restServerCommandOptions.port;

        if (serverCommandOptions.restServerCommandOptions.start) {
            RestServer server = new RestServer(Paths.get(this.appHome), port);
            server.start();
            if (!serverCommandOptions.restServerCommandOptions.background) {
                server.blockUntilShutdown();
            }
            logger.info("Shutting down OpenCGA Storage REST server");
        }

        if (serverCommandOptions.restServerCommandOptions.stop) {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target("http://localhost:" + configuration.getServer().getRest().getPort())
                    .path("opencga")
                    .path("webservices")
                    .path("rest")
                    .path("admin")
                    .path("stop");
            Response response = target.request().get();
            logger.info(response.toString());
        }
    }

    private void grpc() throws Exception {
        if (serverCommandOptions.grpcServerCommandOptions.start) {
            GrpcServer server = new GrpcServer(Paths.get(this.conf));
            server.start();
            if (!serverCommandOptions.grpcServerCommandOptions.background) {
                server.blockUntilShutdown();
            }
            logger.info("Shutting down OpenCGA Storage gRPC server");
        }

        if (serverCommandOptions.grpcServerCommandOptions.stop) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:" + configuration.getServer().getGrpc().getPort())
                    .usePlaintext()
                    .build();
            AdminServiceGrpc.AdminServiceBlockingStub stub = AdminServiceGrpc.newBlockingStub(channel);
            ServiceTypesModel.MapResponse stopResponse = stub.stop(null);
            System.out.println(stopResponse.toString());
        }
    }

}
