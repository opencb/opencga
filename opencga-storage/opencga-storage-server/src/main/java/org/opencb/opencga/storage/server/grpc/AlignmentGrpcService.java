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
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.utils.GrpcServiceUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AlignmentGrpcService extends AlignmentServiceGrpc.AlignmentServiceImplBase {

    private StorageConfiguration storageConfiguration;
    private StorageEngineFactory storageEngineFactory;

    public AlignmentGrpcService(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;

        storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
    }


    @Override
    public void getHeader(AlignmentServiceModel.AlignmentRequest request,
                          StreamObserver<AlignmentServiceModel.StringResponse> responseObserver) {
        try {
            AlignmentStorageEngine alignmentStorageEngine =
                    storageEngineFactory.getAlignmentStorageEngine(storageConfiguration.getDefaultStorageEngineId(), "");

            Path path = Paths.get(request.getFile());
            FileUtils.checkFile(path);

            AlignmentDBAdaptor alignmentDBAdaptor = alignmentStorageEngine.getDBAdaptor();
            AlignmentServiceModel.StringResponse headerResponse = AlignmentServiceModel.StringResponse
                    .newBuilder()
                    .setValue(alignmentDBAdaptor.getHeader(path).first())
                    .build();
            responseObserver.onNext(headerResponse);
            responseObserver.onCompleted();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void get(AlignmentServiceModel.AlignmentRequest request, StreamObserver<Reads.ReadAlignment> responseObserver) {
        try {
            AlignmentStorageEngine alignmentStorageEngine =
                    storageEngineFactory.getAlignmentStorageEngine(storageConfiguration.getDefaultStorageEngineId(), "");

            Path path = Paths.get(request.getFile());
            FileUtils.checkFile(path);

            Query query = GrpcServiceUtils.createQuery(request.getQueryMap());
            QueryOptions queryOptions = GrpcServiceUtils.createQueryOptions(request.getOptionsMap());

            AlignmentDBAdaptor alignmentDBAdaptor = alignmentStorageEngine.getDBAdaptor();
            AlignmentIterator iterator = alignmentDBAdaptor.iterator(path, query, queryOptions);
            while (iterator.hasNext()) {
                Reads.ReadAlignment readAlignment = (Reads.ReadAlignment) iterator.next();
                responseObserver.onNext(readAlignment);
            }
            responseObserver.onCompleted();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void coverage(AlignmentServiceModel.AlignmentRequest request,
                         StreamObserver<AlignmentServiceModel.FloatResponse> responseObserver) {
        try {
            AlignmentStorageEngine alignmentStorageEngine =
                    storageEngineFactory.getAlignmentStorageEngine(storageConfiguration.getDefaultStorageEngineId(), "");

            Path path = Paths.get(request.getFile());
            FileUtils.checkFile(path);

            Query query = GrpcServiceUtils.createQuery(request.getQueryMap());
            String regionStr = query.getString(AlignmentDBAdaptor.QueryParams.REGION.key());
            Region region = Region.parseRegion(regionStr);
            int windowSize = query.getInt(AlignmentDBAdaptor.QueryParams.WINDOW_SIZE.key());

            QueryResult<RegionCoverage> coverageQueryResult = alignmentStorageEngine.getDBAdaptor().coverage(path, region, windowSize);
            for (Float regionCoverage : coverageQueryResult.first().getValues()) {
                AlignmentServiceModel.FloatResponse floatResponse = AlignmentServiceModel.FloatResponse
                        .newBuilder()
                        .setValue(regionCoverage)
                        .build();
                responseObserver.onNext(floatResponse);
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void count(AlignmentServiceModel.AlignmentRequest request, StreamObserver<AlignmentServiceModel.LongResponse> responseObserver) {
        try {
            AlignmentStorageEngine alignmentStorageEngine =
                    storageEngineFactory.getAlignmentStorageEngine(storageConfiguration.getDefaultStorageEngineId(), "");

            Path path = Paths.get(request.getFile());
            Query query = GrpcServiceUtils.createQuery(request.getQueryMap());
            QueryOptions queryOptions = GrpcServiceUtils.createQueryOptions(request.getOptionsMap());

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
                    storageEngineFactory.getAlignmentStorageEngine(storageConfiguration.getDefaultStorageEngineId(), "");

            Path path = Paths.get(request.getFile());
            Query query = GrpcServiceUtils.createQuery(request.getQueryMap());
            QueryOptions queryOptions = GrpcServiceUtils.createQueryOptions(request.getOptionsMap());

            AlignmentDBAdaptor alignmentDBAdaptor = alignmentStorageEngine.getDBAdaptor();
            AlignmentIterator iterator = alignmentDBAdaptor.iterator(path, query, queryOptions, SAMRecord.class);
            while (iterator.hasNext()) {
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
