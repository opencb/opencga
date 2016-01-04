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
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.server.grpc.VariantProto.Variant;

import java.util.Iterator;

import static org.opencb.opencga.storage.server.grpc.GenericServiceModel.*;
import static org.opencb.opencga.storage.server.grpc.VariantServiceGrpc.*;

/**
 * Created by imedina on 29/12/15.
 */
public class VariantGrpcServer extends GenericGrpcServer implements VariantService {

    @Override
    public void count(Request request, StreamObserver<LongResponse> responseObserver) {

    }

    @Override
    public void distinct(Request request, StreamObserver<StringArrayResponse> responseObserver) {

    }

    @Override
    public void get(Request request, StreamObserver<Variant> responseObserver) {
//        var geneDBAdaptor = dbAdaptorFactory.getGeneDBAdaptor(request.getSpecies(), request.getAssembly());
        try {
            VariantStorageManager variantStorageManager = storageManagerFactory.getVariantStorageManager();
            VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor("opencga_test_opencga");

            Query query = createQuery(request);
            QueryOptions queryOptions = createQueryOptions(request);
//        Iterator iterator = geneDBAdaptor.nativeIterator(query, queryOptions);
            Iterator iterator = variantDBAdaptor.iterator(query, queryOptions);
            while (iterator.hasNext()) {
//                Variant variant = (Variant) iterator.next();
                org.opencb.biodata.models.variant.Variant next = (org.opencb.biodata.models.variant.Variant) iterator.next();
                Variant convert = convert(next);
//                responseObserver.onNext(convert(document));
                responseObserver.onNext(convert);
            }
            responseObserver.onCompleted();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | StorageManagerException e) {
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
}
