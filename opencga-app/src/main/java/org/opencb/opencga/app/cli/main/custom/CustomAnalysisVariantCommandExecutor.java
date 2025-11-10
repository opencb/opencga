package org.opencb.opencga.app.cli.main.custom;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.options.AnalysisVariantCommandOptions;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.client.grpc.OpenCGAGrpcClient;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

public class CustomAnalysisVariantCommandExecutor extends CustomCommandExecutor {

    public CustomAnalysisVariantCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                                SessionManager session, String appHome, Logger logger) {
        super(options, token, clientConfiguration, session, appHome, logger);
    }

    public CustomAnalysisVariantCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                                SessionManager session, String appHome, Logger logger, OpenCGAClient openCGAClient) {
        super(options, token, clientConfiguration, session, appHome, logger, openCGAClient);
    }

    private OpenCGAGrpcClient buildGrpcClient() {
        return new OpenCGAGrpcClient(clientConfiguration);
    }

    public RestResponse<Variant> query(AnalysisVariantCommandOptions.QueryCommandOptions commandOptions)
            throws ClientException, InterruptedException, InvalidProtocolBufferException {

        try (OpenCGAGrpcClient grpcClient = buildGrpcClient()) {
            if (grpcClient.isAlive()) {
                // Connecting to the server host and port
                logger.info("Connecting to gRPC server at '{}'", clientConfiguration.getGrpc().getHost());

                JsonFormat.Printer printer = JsonFormat.printer();
                try (PrintStream printStream = new PrintStream(System.out)) {
                    Iterator<VariantProto.Variant> iterator = grpcClient.getAnalysisVariant().query(options, getToken());
                    while (iterator.hasNext()) {
                        VariantProto.Variant variant = iterator.next();
                        printStream.println(printer.print(variant));
                    }
                }

                return new RestResponse<>();
            } else {
                logger.info("Connecting to REST server at '{}'", openCGAClient.getClientConfiguration().getRest().getHosts());
                return openCGAClient.getVariantClient().query(options);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
