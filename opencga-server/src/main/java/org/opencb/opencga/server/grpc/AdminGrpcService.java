/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.server.grpc;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.grpc.stub.StreamObserver;
import org.opencb.biodata.models.common.protobuf.service.ServiceTypesModel;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.server.OpenCGAHealthCheckMonitor;
import org.slf4j.Logger;

/**
 * Created by imedina on 02/01/16.
 */
public class AdminGrpcService extends AdminServiceGrpc.AdminServiceImplBase {

    private final GenericGrpcService grpcService;
    private final GrpcServer grpcServer;
    private final Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

    public AdminGrpcService(GenericGrpcService grpcService, GrpcServer grpcServer) {
        this.grpcService = grpcService;
        this.grpcServer = grpcServer;
    }

    @Override
    public void status(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.MapResponse> responseObserver) {
        OpenCGAResult<OpenCGAHealthCheckMonitor.HealthCheckStatus> result = grpcService.getHealthCheckMonitor().getStatus();
        OpenCGAHealthCheckMonitor.HealthCheckStatus status = result.first();
        if (status.isHealthy()) {
            logger.debug("HealthCheck : " + status);
            ServiceTypesModel.MapResponse.Builder builder = ServiceTypesModel.MapResponse.newBuilder();
            ObjectMap statusMap = new ObjectMap();
            try {
                JacksonUtils.getDefaultObjectMapper().updateValue(statusMap, status);
                for (String key : statusMap.keySet()) {
                    Object value = statusMap.get(key);
                    if (value != null) {
                        builder.putValues(key, value.toString());
                    } else {
                        builder.putValues(key, "null");
                    }
                }
            } catch (JsonMappingException e) {
                throw new RuntimeException(e);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } else {
            logger.error("HealthCheck : " + status);
            responseObserver.onError(new Exception("Some services are not healthy: " + status));
        }

    }

//    @Override
//    public void stop(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.MapResponse> responseObserver) {
//
//        responseObserver.onNext(ServiceTypesModel.MapResponse.getDefaultInstance());
//        responseObserver.onCompleted();
//
//        grpcServer.stop();
//    }

}
