package org.opencb.opencga.client.grpc.clients;

import com.google.common.collect.Iterators;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.server.grpc.GenericServiceModel;
import org.opencb.opencga.server.grpc.VariantServiceGrpc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

public class AnalysisVariantGrpcClient extends AbstractGrpcClient<VariantServiceGrpc.VariantServiceBlockingStub> {

    public AnalysisVariantGrpcClient(Supplier<ManagedChannel> channelSupplier) {
        super(channelSupplier);
    }

    @Override
    protected VariantServiceGrpc.VariantServiceBlockingStub getStub() {
        // We create the gRPC channel to the specified server host and port
        ManagedChannel channel = getManagedChannel();

        // We use a blocking stub to execute the query to gRPC
        return VariantServiceGrpc.newBlockingStub(channel).withCompression("gzip");
    }

    public Iterator<VariantProto.Variant> query(ObjectMap options, String token) {
//        // Connecting to the server host and port
//        logger.info("Connecting to gRPC server at '{}'", clientConfiguration.getGrpc().getHost());


        Map<String, String> optionsMap = new HashMap<>();
        for (String key : options.keySet()) {
            optionsMap.put(key, options.getString(key));
        }

        // We create the OpenCGA gRPC request object with the query, queryOptions and sessionId
        GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                .putAllQuery(optionsMap)
//                .putAllOptions(optionsMap)
                .setToken(token)
                .build();

        try {
            Iterator<GenericServiceModel.VariantResponse> variantIterator = getStub().query(request);
            return Iterators.transform(variantIterator, next -> {
                if (StringUtils.isNotEmpty(next.getError())) {
                    logger.error("gRPC request failed: {}\n{}", next.getErrorFull(), next.getStackTrace());
                    throw new RuntimeException("gRPC request failed: " + next.getError());
                }
                return next.getVariant();
            });
//            JsonFormat.Printer printer = JsonFormat.printer();
//            try (PrintStream printStream = new PrintStream(System.out)) {
//                while (variantIterator.hasNext()) {
//                    GenericServiceModel.VariantResponse next = variantIterator.next();
//                    if (StringUtils.isNotEmpty(next.getError())) {
//                        logger.error("gRPC request failed: {}\n{}", next.getErrorFull(), next.getStackTrace());
//                        continue;
//                    }
//                    VariantProto.Variant variant = next.getVariant();
//                    printStream.println(printer.print(variant));
//                }
//            }
        } catch (StatusRuntimeException e) {
            logger.error("gRPC request failed: {}", e.getStatus(), e);
            throw e;
        } catch (RuntimeException e) {
            logger.error("gRPC request failed: {}", e.getMessage(), e);
            throw e;
        }
    }


}
