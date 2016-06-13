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

package org.opencb.opencga.server.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.common.protobuf.service.ServiceTypesModel;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.util.Iterator;

import static org.opencb.opencga.server.grpc.VariantServiceGrpc.VariantService;

/**
 * Created by imedina on 29/12/15.
 */
public class VariantGrpcService extends GenericGrpcService implements VariantService {

    public VariantGrpcService(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration) {
        super(catalogConfiguration, storageConfiguration);
    }

    @Deprecated
    public VariantGrpcService(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration, String defaultStorageEngine) {
        super(catalogConfiguration, storageConfiguration, defaultStorageEngine);
    }


    @Override
    public void count(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.LongResponse> responseObserver) {
        try {
            // Creating the datastore Query object from the gRPC request Map of Strings
            Query query = createQuery(request);

//            checkAuthorizedHosts(query, request.getIp());
            VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(request);
            QueryResult<Long> queryResult = variantDBAdaptor.count(query);
            responseObserver.onNext(ServiceTypesModel.LongResponse.newBuilder().setValue(queryResult.getResult().get(0)).build());
            responseObserver.onCompleted();
            variantDBAdaptor.close();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | StorageManagerException | IOException e) {
            e.printStackTrace();
        }
//        catch (NotAuthorizedHostException | NotAuthorizedUserException e) {
//            e.printStackTrace();
//        }
    }


    @Override
    public void distinct(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.StringArrayResponse> responseObserver) {

    }


    @Override
    public void get(GenericServiceModel.Request request, StreamObserver<VariantProto.Variant> responseObserver) {
        try {
            // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
            Query query = createQuery(request);
            QueryOptions queryOptions = createQueryOptions(request);

//            checkAuthorizedHosts(query, request.getIp());
            VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(request);
//            Iterator iterator = geneDBAdaptor.nativeIterator(query, queryOptions);
            Iterator iterator = variantDBAdaptor.iterator(query, queryOptions);
            while (iterator.hasNext()) {
                org.opencb.biodata.models.variant.Variant next = (org.opencb.biodata.models.variant.Variant) iterator.next();
                VariantProto.Variant convert = convert(next);
                responseObserver.onNext(convert);
            }
            responseObserver.onCompleted();
            variantDBAdaptor.close();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | StorageManagerException | IOException e) {
            e.printStackTrace();
        }
//        catch (NotAuthorizedHostException e) {
//            e.printStackTrace();
//        } catch (NotAuthorizedUserException e) {
//            e.printStackTrace();
//        }
    }


    @Override
    public void groupBy(GenericServiceModel.Request request, StreamObserver<ServiceTypesModel.GroupResponse> responseObserver) {

    }

    private VariantProto.Variant convert(org.opencb.biodata.models.variant.Variant var) {
        VariantProto.Variant build = VariantProto.Variant.newBuilder()
                .setChromosome(var.getChromosome())
                .setStart(var.getStart())
                .setEnd(var.getEnd())
                .setLength(var.getLength())
                .setReference(var.getReference())
                .setAlternate(var.getAlternate())
                .setId(var.getId())
                .addAllNames(var.getNames())
                .build();

        return build;
    }

    private VariantDBAdaptor getVariantDBAdaptor(GenericServiceModel.Request request)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, StorageManagerException {
        // Setting storageEngine and database parameters. If the storageEngine is not provided then the server default is used
        String storageEngine = defaultStorageEngine;
        if (StringUtils.isNotEmpty(request.getStorageEngine())) {
            storageEngine = request.getStorageEngine();
        }

        String database = storageConfiguration.getStorageEngine(storageEngine).getVariant().getOptions().getString("database.name");
        if (StringUtils.isNotEmpty(request.getDatabase())) {
            database = request.getDatabase();
        }

        // Creating the VariantDBAdaptor to the parsed storageEngine and database
        long start = System.currentTimeMillis();
        VariantStorageManager variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(database);
        logger.debug("Connection to {}:{} in {}ms", storageEngine, database, System.currentTimeMillis() - start);

        return variantDBAdaptor;
    }
}
