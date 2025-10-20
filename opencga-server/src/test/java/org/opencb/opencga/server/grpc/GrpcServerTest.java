package org.opencb.opencga.server.grpc;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.common.protobuf.service.ServiceTypesModel;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.opencga.core.common.ExceptionUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GrpcServerTest {

    private Server server;
    private VariantServiceGrpc.VariantServiceBlockingStub variantServiceBlockingStub;
    private AdminServiceGrpc.AdminServiceBlockingStub adminServiceBlockingStub;
    private int port;

    @Before
    public void setUp() throws Exception {
        port = 9999;
        server = ServerBuilder.forPort(port)
                .addService(new TestAdminGrpcService())
                .addService(new TestVariantGrpcService())
                .build()
                .start();


        // We create the gRPC channel to the specified server host and port
        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:" + port)
                .usePlaintext()
                .build();

        // We use a blocking stub to execute the query to gRPC
        variantServiceBlockingStub = VariantServiceGrpc.newBlockingStub(channel);
        adminServiceBlockingStub = AdminServiceGrpc.newBlockingStub(channel);

    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.shutdown();
            server.awaitTermination();
        }
    }

    @Test
    public void testIsAlive() throws Exception {
        adminServiceBlockingStub.ping(GenericServiceModel.Request.newBuilder().setUser("me").build());
    }

    @Test(expected = RuntimeException.class)
    public void testIsAliveFail() {
        // We create the gRPC channel to the specified server host and port
        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:" + (port + 1))
                .usePlaintext()
                .build();

        AdminServiceGrpc.newBlockingStub(channel).ping(GenericServiceModel.Request.newBuilder().setUser("me").build());
    }

    @Test
    public void testStreamQuery() throws Exception {
        // Test a query that streams 10 variants

        Map<String, String> queryMap = new HashMap<>();
        queryMap.put("n", "10");

        // We create the OpenCGA gRPC request object with the query, queryOptions and sessionId
        GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                .putAllQuery(queryMap)
                .build();

        Iterator<GenericServiceModel.VariantResponse> variantIterator = variantServiceBlockingStub.query(request);
        int count = 0;
        while (variantIterator.hasNext()) {
            GenericServiceModel.VariantResponse next = variantIterator.next();
            VariantProto.Variant variant = next.getVariant();
            System.out.println("Received variant: " + variant.getId());
            count++;
        }
        Assert.assertEquals(10, count);
    }

    @Test
    public void testStreamQueryWithFailure() throws Exception {
        // Test a query that streams 10 variants but fails at variant 5

        Map<String, String> queryMap = new HashMap<>();
        queryMap.put("n", "10");
        queryMap.put("nFail", "5");

        // We create the OpenCGA gRPC request object with the query, queryOptions and sessionId
        GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                .putAllQuery(queryMap)
                .build();

        Iterator<GenericServiceModel.VariantResponse> variantIterator = variantServiceBlockingStub.query(request);
        int count = 0;
        boolean failed = false;
        try {
            while (variantIterator.hasNext()) {
                GenericServiceModel.VariantResponse next = variantIterator.next();
                if (StringUtils.isNotEmpty(next.getError())) {
                    failed = true;
                    Assert.assertEquals("Simulated failure at variant 5", next.getError());
                    System.out.println("Received error at variant " + next.getCount() + ": " + next.getError());
                    System.out.println("Full error: " + next.getErrorFull());
                    System.out.println("Stack trace: " + next.getStackTrace());
                } else {
                    VariantProto.Variant variant = next.getVariant();
                    System.out.println("Received variant: " + variant.getId());
                    count++;
                }
            }
        } catch (StatusRuntimeException e) {
            Assert.assertTrue(failed);
        }
        Assert.assertTrue(failed);
        Assert.assertEquals(5, count);
    }

    public static class TestAdminGrpcService extends AdminServiceGrpc.AdminServiceImplBase {

        @Override
        public void ping(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.MapResponse> responseObserver) {
            responseObserver.onNext(ServiceTypesModel.MapResponse.newBuilder().putValues("ping", "pong").build());
            responseObserver.onCompleted();
        }

        @Override
        public void status(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.MapResponse> responseObserver) {
            ServiceTypesModel.MapResponse response = ServiceTypesModel.MapResponse.newBuilder()
                    .putValues("status", "running")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    public static class TestVariantGrpcService extends VariantServiceGrpc.VariantServiceImplBase {
        @Override
        public void query(GenericServiceModel.Request request, StreamObserver<GenericServiceModel.VariantResponse> responseObserver) {
            int n = Integer.parseInt(request.getQueryMap().get("n"));
            int nFail = Integer.parseInt(request.getQueryMap().getOrDefault("nFail", "-1"));
            for (int i = 0; i < n; i++) {
                if (i == nFail) {
                    RuntimeException e = new RuntimeException("Simulated failure at variant " + i);
                    GenericServiceModel.VariantResponse response = GenericServiceModel.VariantResponse.newBuilder()
                            .setCount(i)
                            .setError(e.getMessage())
                            .setErrorFull(ExceptionUtils.prettyExceptionMessage(e))
                            .setStackTrace(ExceptionUtils.prettyExceptionStackTrace(e))
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
                    return;
                }
                VariantProto.Variant variant = VariantProto.Variant.newBuilder()
                        .setId("var" + i)
                        .build();
                GenericServiceModel.VariantResponse response = GenericServiceModel.VariantResponse.newBuilder()
                        .setVariant(variant)
                        .setCount(i)
                        .build();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
        }
    }
}
