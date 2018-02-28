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

package org.opencb.opencga.storage.server.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.proto.VariantAvroToVariantProtoConverter;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.io.IOException;


/**
 * Created by imedina on 29/12/15.
 */
public class VariantGrpcService extends VariantServiceGrpc.VariantServiceImplBase {

    private StorageConfiguration storageConfiguration;
    private StorageEngineFactory storageEngineFactory;

    public VariantGrpcService(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;

        storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
    }


    @Override
    public void count(GenericServiceModel.Request request, StreamObserver<GenericServiceModel.LongResponse> responseObserver) {
        try {
            // Creating the datastore Query object from the gRPC request Map of Strings
            Query query = GrpcServiceUtils.createQuery(request.getQueryMap());

            VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(request);
            QueryResult<Long> queryResult = variantDBAdaptor.count(query);
            responseObserver.onNext(GenericServiceModel.LongResponse.newBuilder().setValue(queryResult.getResult().get(0)).build());
            responseObserver.onCompleted();
            variantDBAdaptor.close();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | StorageEngineException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void get(GenericServiceModel.Request request, StreamObserver<VariantProto.Variant> responseObserver) {
        try {
            // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
            Query query = GrpcServiceUtils.createQuery(request.getOptionsMap());
            QueryOptions queryOptions = GrpcServiceUtils.createQueryOptions(request.getOptionsMap());

            VariantAvroToVariantProtoConverter converter = new VariantAvroToVariantProtoConverter();
            VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(request);
//            Iterator iterator = variantDBAdaptor.iterator(query, queryOptions);
            try (VariantDBIterator iterator = variantDBAdaptor.iterator(query, queryOptions)) {
                while (iterator.hasNext()) {
                    org.opencb.biodata.models.variant.Variant variant = (org.opencb.biodata.models.variant.Variant) iterator.next();
                    responseObserver.onNext(converter.convert(variant));
                }
            }
            responseObserver.onCompleted();
            variantDBAdaptor.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getAsJson(GenericServiceModel.Request request, StreamObserver<GenericServiceModel.StringResponse> responseObserver) {

    }

    @Override
    public void groupBy(GenericServiceModel.Request request, StreamObserver<GenericServiceModel.GroupResponse> responseObserver) {

    }

    private VariantDBAdaptor getVariantDBAdaptor(GenericServiceModel.Request request)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, StorageEngineException {
        // Setting storageEngine and database parameters. If the storageEngine is not provided then the server default is used
        String storageEngine = storageConfiguration.getDefaultStorageEngineId();
        if (StringUtils.isNotEmpty(request.getStorageEngine())) {
            storageEngine = request.getStorageEngine();
        }

        String database = storageConfiguration.getStorageEngine(storageEngine).getVariant().getOptions().getString("database.name");
        if (StringUtils.isNotEmpty(request.getDatabase())) {
            database = request.getDatabase();
        }

        // Creating the VariantDBAdaptor to the parsed storageEngine and database
        VariantDBAdaptor variantDBAdaptor = storageEngineFactory.getVariantStorageEngine(storageEngine, database).getDBAdaptor();
//        logger.debug("Connection to {}:{} in {}ms", storageEngine, database, System.currentTimeMillis() - start);

        return variantDBAdaptor;
    }
}
