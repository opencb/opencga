package org.opencb.opencga.client.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.client.grpc.clients.AnalysisVariantGrpcClient;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.server.grpc.GenericServiceModel;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class OpenCGAGrpcClient implements Closeable {

    private final AtomicReference<ManagedChannel> channel = new AtomicReference<ManagedChannel>();
    private final org.opencb.opencga.core.config.client.ClientConfiguration clientConfiguration;
    private final AnalysisVariantGrpcClient analysisVariant;
    private final Logger logger = org.slf4j.LoggerFactory.getLogger(OpenCGAGrpcClient.class);

    public OpenCGAGrpcClient(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        analysisVariant = new AnalysisVariantGrpcClient(this::getManagedChannel);
    }

    public AnalysisVariantGrpcClient getAnalysisVariant() {
        return analysisVariant;
    }

    @Override
    public void close() throws IOException {
        if (channel.get() != null && !channel.get().isShutdown()) {
            try {
                channel.get().shutdown().awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            channel.set(null);
        }
    }

    private ManagedChannel getManagedChannel() {
        if (channel.get() == null) {
            return createManagedChannel();
        }
        return channel.get();
    }

    private synchronized ManagedChannel createManagedChannel() {
        if (channel.get() != null) {
            return channel.get();
        }
        String grpcServerHost = clientConfiguration.getGrpc().getHost();
        URI uri = UriUtils.createUriSafe(grpcServerHost);
        boolean plainText = false;
        if (StringUtils.isNotEmpty(uri.getScheme())) {
            if (uri.getScheme().equals("grpc+http") || uri.getScheme().equals("http")) {
                plainText = true;
                int port = uri.getPort();
                if (port <= 0) {
                    port = 80;
                }
                grpcServerHost = uri.getHost() + ":" + port;
            }
        }
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(grpcServerHost);
        if (plainText) {
            channelBuilder.usePlaintext();
        }
        channel.set(channelBuilder.build());
        return channel.get();
    }

    public boolean isAlive() {
        try {
            ManagedChannel channel = getManagedChannel();
            AdminServiceGrpc.newBlockingStub(channel).ping(GenericServiceModel.Request.newBuilder().build());
            return true;
        } catch (RuntimeException e) {
            logger.debug("GRPC server is not alive", e);
            return false;
        }
    }
}
