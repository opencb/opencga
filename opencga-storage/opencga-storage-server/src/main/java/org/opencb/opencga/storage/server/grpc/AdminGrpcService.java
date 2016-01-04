/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.server.grpc;

import io.grpc.stub.StreamObserver;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.server.grpc.AdminServiceGrpc.AdminService;

/**
 * Created by imedina on 02/01/16.
 */
public class AdminGrpcService extends GenericGrpcService implements AdminService {

    private GrpcStorageServer grpcServer;


    public AdminGrpcService(StorageConfiguration storageConfiguration, GrpcStorageServer grpcServer) {
        super(storageConfiguration);
        this.grpcServer = grpcServer;
    }

    @Override
    public void status(GenericServiceModel.Request request, StreamObserver<GenericServiceModel.MapResponse> responseObserver) {

    }

    @Override
    public void stop(GenericServiceModel.Request request, StreamObserver<GenericServiceModel.MapResponse> responseObserver) {

        responseObserver.onNext(GenericServiceModel.MapResponse.getDefaultInstance());
        responseObserver.onCompleted();

        grpcServer.stop();
    }

}
