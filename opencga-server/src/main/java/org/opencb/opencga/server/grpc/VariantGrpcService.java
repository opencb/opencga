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
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.core.common.ExceptionUtils;
import org.opencb.opencga.server.grpc.VariantServiceGrpc.VariantServiceImplBase;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.server.grpc.GenericServiceModel.Request;
import static org.opencb.opencga.server.grpc.GenericServiceModel.VariantResponse;
import static org.opencb.opencga.server.grpc.VariantServiceGrpc.getQueryMethod;

/**
 * Created by imedina on 29/12/15.
 */
public class VariantGrpcService extends VariantServiceImplBase {

    private final GenericGrpcService genericGrpcService;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final VariantStorageManager variantStorageManager;

    public VariantGrpcService(GenericGrpcService genericGrpcService) {
        this.genericGrpcService = genericGrpcService;
        variantStorageManager = genericGrpcService.getVariantStorageManager();
    }

    @Override
    public void query(Request request, StreamObserver<VariantResponse> responseObserver) {
        genericGrpcService.run(getQueryMethod(), request, responseObserver, (query, queryOptions) -> {
            Query variantQuery = VariantStorageManager.getVariantQuery(queryOptions);
            query.putAll(variantQuery);
//            Map<String, String> queryMap = new HashMap<>();
//            Map<String, String> queryOptionsMap = new HashMap<>();
//            for (String key : queryOptions.keySet()) {
//                if (query.containsKey(key)) {
//                    queryMap.put(key, query.getString(key));
//                } else {
//                    queryOptionsMap.put(key, queryOptions.getString(key));
//                }
//            }

            VariantAvroToVariantProtoConverter converter = new VariantAvroToVariantProtoConverter();
            int i = 0;
            List<org.opencb.opencga.server.grpc.GenericServiceModel.Event> events = null;
            try (VariantDBIterator iterator = variantStorageManager.iterator(query, queryOptions, request.getToken())) {
                if (iterator.getEvents() != null) {
                    events = new ArrayList<>(iterator.getEvents().size());
                    for (Event event : iterator.getEvents()) {
                        org.opencb.opencga.server.grpc.GenericServiceModel.Event.Builder eventB =
                                org.opencb.opencga.server.grpc.GenericServiceModel.Event.newBuilder();
                        if (event.getMessage() != null) {
                            eventB.setMessage(event.getMessage());
                        }
                        if (event.getType() != null) {
                            eventB.setType(event.getType().name());
                        }
                        if (event.getName() != null) {
                            eventB.setName(event.getName());
                        }
                        if (event.getId() != null) {
                            eventB.setId(event.getId());
                        }
                        eventB.setCode(event.getCode());
                        events.add(eventB.build());
                    }
                }
                while (iterator.hasNext() && !genericGrpcService.isCancelled(responseObserver)) {
                    Variant variant = iterator.next();
                    VariantProto.Variant variantProto = converter.convert(variant);
                    VariantResponse.Builder responseBuilder = VariantResponse.newBuilder();
                    if (events != null) {
                        responseBuilder.addAllEvent(events);
                        events = null;
                    }
                    VariantResponse response = responseBuilder
                            .setVariant(variantProto)
                            .setCount(i++)
                            .build();
                    responseObserver.onNext(response);
                }
            } catch (Exception e) {
                VariantResponse.Builder responseBuilder = VariantResponse.newBuilder();
                if (events != null) {
                    responseBuilder.addAllEvent(events);
                }
                VariantResponse response = responseBuilder
                        .setError(e.getMessage())
                        .setErrorFull(ExceptionUtils.prettyExceptionMessage(e))
                        .setStackTrace(ExceptionUtils.prettyExceptionStackTrace(e))
                        .build();
                responseObserver.onNext(response);
                throw e;
            }
            return i;
        });
    }

}
