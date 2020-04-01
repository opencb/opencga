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

import io.grpc.stub.StreamObserver;
import org.opencb.biodata.models.common.protobuf.service.ServiceTypesModel;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

/**
 * Created by imedina on 02/01/16.
 */
public class AdminGrpcService extends AdminServiceGrpc.AdminServiceImplBase {

    private GrpcServer grpcServer;


    public AdminGrpcService(Configuration configuration, StorageConfiguration storageConfiguration, GrpcServer grpcServer) {
//        super(catalogConfiguration, storageConfiguration);

        this.grpcServer = grpcServer;
    }

    @Override
    public void status(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.MapResponse> responseObserver) {
        responseObserver.onNext(ServiceTypesModel.MapResponse.newBuilder().putValues("status", "alive").build());
        responseObserver.onCompleted();
    }

    @Override
    public void stop(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.MapResponse> responseObserver) {

        responseObserver.onNext(ServiceTypesModel.MapResponse.getDefaultInstance());
        responseObserver.onCompleted();

        grpcServer.stop();
    }

}
