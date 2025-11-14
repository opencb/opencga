package org.opencb.opencga.client.grpc.clients;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import org.slf4j.Logger;

import java.util.function.Supplier;

public abstract class AbstractGrpcClient<STUB extends AbstractStub<STUB>> {

    protected final Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());
    private final Logger privateLogger = org.slf4j.LoggerFactory.getLogger(AbstractGrpcClient.class);
    private final Supplier<ManagedChannel> channelSupplier;

    protected AbstractGrpcClient(Supplier<ManagedChannel> channelSupplier) {
        this.channelSupplier = channelSupplier;
    }

    protected abstract STUB getStub();

    protected ManagedChannel getManagedChannel() {
        return channelSupplier.get();
    }

}
