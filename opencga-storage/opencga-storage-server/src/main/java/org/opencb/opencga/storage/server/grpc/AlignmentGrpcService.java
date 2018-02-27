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

import ga4gh.Reads;
import htsjdk.samtools.SAMRecord;
import io.grpc.stub.StreamObserver;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AlignmentGrpcService extends AlignmentServiceGrpc.AlignmentServiceImplBase {

    private GenericGrpcService genericGrpcService;

    public AlignmentGrpcService(StorageConfiguration storageConfiguration) {
//        super(storageConfiguration);
        genericGrpcService = new GenericGrpcService(storageConfiguration);
    }

    @Override
    public void get(AlignmentServiceModel.AlignmentRequest request, StreamObserver<Reads.ReadAlignment> responseObserver) {
        try {
            AlignmentStorageEngine alignmentStorageEngine =
                    GenericGrpcService.storageEngineFactory.getAlignmentStorageEngine(genericGrpcService.defaultStorageEngine, "");

            Path path = Paths.get(request.getFile());
            Query query = genericGrpcService.createQuery(request.getQueryMap());
            QueryOptions queryOptions = genericGrpcService.createQueryOptions(request.getOptionsMap());

            int limit = queryOptions.getInt(QueryOptions.LIMIT) > 0 ? queryOptions.getInt(QueryOptions.LIMIT) : Integer.MAX_VALUE;
            int count = 0;
            AlignmentDBAdaptor alignmentDBAdaptor = alignmentStorageEngine.getDBAdaptor();
            AlignmentIterator iterator = alignmentDBAdaptor.iterator(path, query, queryOptions);
            while (iterator.hasNext() && count++ < limit) {
                Reads.ReadAlignment readAlignment = (Reads.ReadAlignment) iterator.next();
                responseObserver.onNext(readAlignment);
            }
            responseObserver.onCompleted();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void coverage(AlignmentServiceModel.AlignmentRequest request,
                         StreamObserver<AlignmentServiceModel.IntArrayResponse> responseObserver) {
        super.coverage(request, responseObserver);
    }

    @Override
    public void count(AlignmentServiceModel.AlignmentRequest request, StreamObserver<AlignmentServiceModel.LongResponse> responseObserver) {
        try {
            AlignmentStorageEngine alignmentStorageEngine =
                    GenericGrpcService.storageEngineFactory.getAlignmentStorageEngine(genericGrpcService.defaultStorageEngine, "");

            Path path = Paths.get(request.getFile());
            Query query = genericGrpcService.createQuery(request.getQueryMap());
            QueryOptions queryOptions = genericGrpcService.createQueryOptions(request.getOptionsMap());

            AlignmentDBAdaptor alignmentDBAdaptor = alignmentStorageEngine.getDBAdaptor();
            QueryResult<Long> queryResult = alignmentDBAdaptor.count(path, query, queryOptions);
            AlignmentServiceModel.LongResponse longResponse =
                    AlignmentServiceModel.LongResponse.newBuilder().setValue(queryResult.first()).build();
            responseObserver.onNext(longResponse);
            responseObserver.onCompleted();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getAsSam(AlignmentServiceModel.AlignmentRequest request,
                         StreamObserver<AlignmentServiceModel.StringResponse> responseObserver) {
        try {
            AlignmentStorageEngine alignmentStorageEngine =
                    GenericGrpcService.storageEngineFactory.getAlignmentStorageEngine(genericGrpcService.defaultStorageEngine, "");

            Path path = Paths.get(request.getFile());
            Query query = genericGrpcService.createQuery(request.getQueryMap());
            QueryOptions queryOptions = genericGrpcService.createQueryOptions(request.getOptionsMap());

            int limit = queryOptions.getInt(QueryOptions.LIMIT) > 0 ? queryOptions.getInt(QueryOptions.LIMIT) : Integer.MAX_VALUE;
            int count = 0;
            AlignmentDBAdaptor alignmentDBAdaptor = alignmentStorageEngine.getDBAdaptor();
            AlignmentIterator iterator = alignmentDBAdaptor.iterator(path, query, queryOptions, SAMRecord.class);
            while (iterator.hasNext() && count++ < limit) {
                SAMRecord samRecord = (SAMRecord) iterator.next();
                AlignmentServiceModel.StringResponse stringResponse =
                        AlignmentServiceModel.StringResponse.newBuilder().setValue(samRecord.getSAMString()).build();
                responseObserver.onNext(stringResponse);
            }
            responseObserver.onCompleted();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException e) {
            e.printStackTrace();
        }
    }
}
