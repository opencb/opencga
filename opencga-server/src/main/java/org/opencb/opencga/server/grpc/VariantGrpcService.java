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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.proto.VariantAvroToVariantProtoConverter;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opencb.opencga.server.grpc.VariantServiceGrpc.getQueryMethod;

/**
 * Created by imedina on 29/12/15.
 */
public class VariantGrpcService extends VariantServiceGrpc.VariantServiceImplBase {

    private final GenericGrpcService genericGrpcService;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final VariantStorageManager variantStorageManager;

    public VariantGrpcService(GenericGrpcService genericGrpcService) {
        this.genericGrpcService = genericGrpcService;
        variantStorageManager = genericGrpcService.getVariantStorageManager();
    }

    @Override
    public void query(GenericServiceModel.Request request, StreamObserver<VariantProto.Variant> responseObserver) {
        genericGrpcService.run(getQueryMethod(), request, (query, queryOptions) -> {
            VariantAvroToVariantProtoConverter converter = new VariantAvroToVariantProtoConverter();

            try (VariantDBIterator iterator = variantStorageManager.iterator(query, queryOptions, request.getToken())) {
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();
                    responseObserver.onNext(converter.convert(variant));
                }
            } catch (Exception e) {
                responseObserver.onError(e);
                throw e;
            }
            responseObserver.onCompleted();
        });
    }

}
