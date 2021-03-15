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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.util.Iterator;

import static org.opencb.opencga.storage.server.grpc.GenericServiceModel.*;
import static org.opencb.opencga.storage.server.grpc.VariantProto.Variant;


/**
 * Created by imedina on 29/12/15.
 */
public class VariantGrpcService extends VariantServiceGrpc.VariantServiceImplBase {

    private GenericGrpcService genericGrpcService;

    public VariantGrpcService(StorageConfiguration storageConfiguration) {
//        super(storageConfiguration);
        genericGrpcService = new GenericGrpcService(storageConfiguration);
    }

    @Deprecated
    public VariantGrpcService(StorageConfiguration storageConfiguration, String defaultStorageEngine) {
//        super(storageConfiguration, defaultStorageEngine);
        genericGrpcService = new GenericGrpcService(storageConfiguration);
    }


    @Override
    public void count(Request request, StreamObserver<GenericServiceModel.LongResponse> responseObserver) {
        try {
            // Creating the datastore Query object from the gRPC request Map of Strings
            Query query = genericGrpcService.createQuery(request);

//            checkAuthorizedHosts(query, request.getIp());
            VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(request);
            DataResult<Long> queryResult = variantDBAdaptor.count(query);
            responseObserver.onNext(LongResponse.newBuilder().setValue(queryResult.getResults().get(0)).build());
            responseObserver.onCompleted();
            variantDBAdaptor.close();
        } catch (IOException | StorageEngineException e) {
            // FIXME!
            e.printStackTrace();
        }
    }

    @Override
    public void distinct(Request request, StreamObserver<StringArrayResponse> responseObserver) {

    }

    @Override
    public void get(Request request, StreamObserver<Variant> responseObserver) {
        try {
            // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
            Query query = genericGrpcService.createQuery(request);
            QueryOptions queryOptions = genericGrpcService.createQueryOptions(request);

//            checkAuthorizedHosts(query, request.getIp());
            VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(request);
//            Iterator iterator = geneDBAdaptor.nativeIterator(query, queryOptions);
            Iterator iterator = variantDBAdaptor.iterator(query, queryOptions);
            while (iterator.hasNext()) {
                org.opencb.biodata.models.variant.Variant next = (org.opencb.biodata.models.variant.Variant) iterator.next();
                Variant convert = convert(next);
                responseObserver.onNext(convert);
            }
            responseObserver.onCompleted();
            variantDBAdaptor.close();
        } catch (StorageEngineException | IOException e) {
            // FIXME!
            e.printStackTrace();
        }
    }

    @Override
    public void getJson(Request request, StreamObserver<StringResponse> responseObserver) {

    }

    @Override
    public void groupBy(Request request, StreamObserver<GroupResponse> responseObserver) {

    }

    private Variant convert(org.opencb.biodata.models.variant.Variant var) {
        Variant build = Variant.newBuilder()
                .setChromosome(var.getChromosome())
                .setStart(var.getStart())
                .setEnd(var.getEnd())
                .setLength(var.getLength())
                .setReference(var.getReference())
                .setAlternate(var.getAlternate())
                .addAllIds(var.getIds())
                .build();

        return build;
    }

    private VariantDBAdaptor getVariantDBAdaptor(Request request)
            throws StorageEngineException {
        // Setting storageEngine and database parameters. If the storageEngine is not provided then the server default is used
        String storageEngine = genericGrpcService.getDefaultStorageEngine();
        if (StringUtils.isNotEmpty(request.getStorageEngine())) {
            storageEngine = request.getStorageEngine();
        }

        String database = genericGrpcService
                .getStorageConfiguration().getVariantEngine(storageEngine).getOptions().getString("database.name");
        if (StringUtils.isNotEmpty(request.getDatabase())) {
            database = request.getDatabase();
        }

        // Creating the VariantDBAdaptor to the parsed storageEngine and database
        VariantDBAdaptor variantDBAdaptor = GenericGrpcService.storageEngineFactory
                .getVariantStorageEngine(storageEngine, database).getDBAdaptor();
//        logger.debug("Connection to {}:{} in {}ms", storageEngine, database, System.currentTimeMillis() - start);

        return variantDBAdaptor;
    }
}
