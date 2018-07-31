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

package org.opencb.opencga.server.grpc;

import io.grpc.stub.StreamObserver;
import org.opencb.biodata.models.common.protobuf.service.ServiceTypesModel;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.proto.VariantAvroToVariantProtoConverter;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 29/12/15.
 */
public class VariantGrpcService extends VariantServiceGrpc.VariantServiceImplBase {

    private GenericGrpcService genericGrpcService;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public VariantGrpcService(Configuration configuration, StorageConfiguration storageConfiguration) {
        genericGrpcService = new GenericGrpcService(configuration, storageConfiguration);
    }

    @Override
    public void count(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.LongResponse> responseObserver) {
        try {
            Query query = genericGrpcService.createQuery(request);
            logger.info("Count variants query : {} " + query.toJson());
            QueryResult<Long> count = genericGrpcService.variantStorageManager.count(query, request.getSessionId());
            responseObserver.onNext(ServiceTypesModel.LongResponse.newBuilder().setValue(count.getResult().get(0)).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error on count", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void distinct(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.StringArrayResponse> responseObserver) {
        super.distinct(request, responseObserver);
    }

    @Override
    public void get(GenericServiceModel.Request request, StreamObserver<VariantProto.Variant> responseObserver) {
        try {
            VariantAvroToVariantProtoConverter converter = new VariantAvroToVariantProtoConverter();
            Query query = genericGrpcService.createQuery(request);
            QueryOptions queryOptions = genericGrpcService.createQueryOptions(request);
            logger.info("Get variants query : {} , queryOptions : {}" , query.toJson(), queryOptions.toJson());
            try (VariantDBIterator iterator = genericGrpcService.variantStorageManager.iterator(query, queryOptions, request.getSessionId())) {
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();
                    responseObserver.onNext(converter.convert(variant));
                }
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error on get variants", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void groupBy(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.GroupResponse> responseObserver) {
        super.groupBy(request, responseObserver);
    }

}
