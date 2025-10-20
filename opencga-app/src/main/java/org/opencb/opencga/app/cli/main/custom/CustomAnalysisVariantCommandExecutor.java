package org.opencb.opencga.app.cli.main.custom;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.cli.main.options.AnalysisVariantCommandOptions;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.server.grpc.GenericServiceModel;
import org.opencb.opencga.server.grpc.VariantServiceGrpc;
import org.slf4j.Logger;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CustomAnalysisVariantCommandExecutor extends CustomCommandExecutor {

    public CustomAnalysisVariantCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                                SessionManager session, String appHome, Logger logger) {
        super(options, token, clientConfiguration, session, appHome, logger);
    }

    public CustomAnalysisVariantCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                                SessionManager session, String appHome, Logger logger, OpenCGAClient openCGAClient) {
        super(options, token, clientConfiguration, session, appHome, logger, openCGAClient);
    }

    public boolean isGrpcAlive() {
        String grpcServerHost = clientConfiguration.getGrpc().getHost();
        ManagedChannel channel = ManagedChannelBuilder.forTarget(grpcServerHost)
                .usePlaintext()
                .build();

        try {
            AdminServiceGrpc.newBlockingStub(channel).ping(GenericServiceModel.Request.newBuilder().build());
            return true;
        } catch (RuntimeException e) {
            logger.debug("GRPC server is not alive");
            return false;
        }
    }

    public RestResponse<Variant> query(AnalysisVariantCommandOptions.QueryCommandOptions commandOptions)
            throws ClientException, InterruptedException, InvalidProtocolBufferException {

        if (isGrpcAlive()) {
            // Connecting to the server host and port
            String grpcServerHost = clientConfiguration.getGrpc().getHost();
            logger.debug("Connecting to gRPC server at '{}'", grpcServerHost);

            // We create the gRPC channel to the specified server host and port
            ManagedChannel channel = ManagedChannelBuilder.forTarget(grpcServerHost)
                    .usePlaintext()
                    .build();

            // We use a blocking stub to execute the query to gRPC
            VariantServiceGrpc.VariantServiceBlockingStub variantServiceBlockingStub = VariantServiceGrpc.newBlockingStub(channel);

            Query query = VariantStorageManager.getVariantQuery(options);
            Map<String, String> queryMap = new HashMap<>();
            Map<String, String> queryOptionsMap = new HashMap<>();
            for (String key : options.keySet()) {
                if (query.containsKey(key)) {
                    queryMap.put(key, query.getString(key));
                } else {
                    queryOptionsMap.put(key, options.getString(key));
                }
            }

            // We create the OpenCGA gRPC request object with the query, queryOptions and sessionId
            GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                    .putAllQuery(queryMap)
                    .putAllOptions(queryOptionsMap)
                    .setToken(getToken())
                    .build();

            try {
                Iterator<GenericServiceModel.VariantResponse> variantIterator = variantServiceBlockingStub
//                        .withInterceptors(new ExceptionLoggingInterceptor())
                        .withCompression("gzip")
                        .query(request);
                JsonFormat.Printer printer = JsonFormat.printer();
                try (PrintStream printStream = new PrintStream(System.out)) {
                    while (variantIterator.hasNext()) {
                        GenericServiceModel.VariantResponse next = variantIterator.next();
                        if (StringUtils.isNotEmpty(next.getError())) {
                            logger.error("gRPC request failed: {}\n{}", next.getErrorFull(), next.getStackTrace());
                            continue;
                        }
                        VariantProto.Variant variant = next.getVariant();
                        printStream.println(printer.print(variant));
                    }
                }
            } catch (StatusRuntimeException e) {
                logger.error("gRPC request failed: {}", e.getStatus(), e);
            } catch (RuntimeException e) {
                logger.error("gRPC request failed: {}", e.getMessage(), e);
            } finally {
                if (!channel.isShutdown()) {
                    channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
                }
            }

            return null;
        } else {
            return openCGAClient.getVariantClient().query(options);
        }
    }

}
