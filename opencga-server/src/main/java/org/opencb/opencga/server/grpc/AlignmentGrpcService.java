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

import ga4gh.Reads;
import htsjdk.samtools.SAMRecord;
import io.grpc.stub.StreamObserver;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;
import org.opencb.opencga.storage.core.utils.GrpcServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pfurio on 26/10/16.
 */
public class AlignmentGrpcService extends AlignmentServiceGrpc.AlignmentServiceImplBase {

    private GenericGrpcService genericGrpcService;
    private AlignmentStorageManager alignmentStorageManager;

    private Logger logger;

    public AlignmentGrpcService(Configuration configuration, StorageConfiguration storageConfiguration) {
        genericGrpcService = new GenericGrpcService(configuration, storageConfiguration);
        alignmentStorageManager = new AlignmentStorageManager(genericGrpcService.catalogManager, GenericGrpcService.storageEngineFactory);

        logger = LoggerFactory.getLogger(getClass());
    }


    @Override
    public void get(AlignmentServiceModel.AlignmentRequest request, StreamObserver<Reads.ReadAlignment> responseObserver) {
        // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
        Query query = GrpcServiceUtils.createQuery(request.getOptionsMap());
        QueryOptions queryOptions = GrpcServiceUtils.createQueryOptions(request.getOptionsMap());

//        String fileIdStr = query.getString("fileId");
        String fileIdStr = request.getFile();
        String studyIdStr = query.getString("study");
        String sessionId = query.getString("sid");

        try (AlignmentIterator<Reads.ReadAlignment> iterator =
                     alignmentStorageManager.iterator(studyIdStr, fileIdStr, query, queryOptions, sessionId, Reads.ReadAlignment.class)) {
            while (iterator.hasNext()) {
                responseObserver.onNext(iterator.next());
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error in get: {}", e.toString());
            e.printStackTrace();
        }
    }

    @Override
    public void coverage(AlignmentServiceModel.AlignmentRequest request,
                         StreamObserver<AlignmentServiceModel.FloatResponse> responseObserver) {
        try {
            Query query = GrpcServiceUtils.createQuery(request.getQueryMap());
            String studyIdStr = query.getString("study");
            String fileIdStr = request.getFile();
            String regionStr = query.getString(AlignmentDBAdaptor.QueryParams.REGION.key());
            Region region = Region.parseRegion(regionStr);
            int windowSize = query.getInt(AlignmentDBAdaptor.QueryParams.WINDOW_SIZE.key());
            String sessionId = query.getString("sid");

            QueryResult<RegionCoverage> coverageQueryResult =
                    alignmentStorageManager.coverage(studyIdStr, fileIdStr, region, windowSize, sessionId);
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
            // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
            Query query = GrpcServiceUtils.createQuery(request.getOptionsMap());
            QueryOptions queryOptions = GrpcServiceUtils.createQueryOptions(request.getOptionsMap());

//            String fileIdStr = query.getString("fileId");
            String fileIdStr = request.getFile();
            String studyIdStr = query.getString("study");
            String sessionId = query.getString("sid");

            QueryResult<Long> countQueryResult = alignmentStorageManager.count(studyIdStr, fileIdStr, query, queryOptions, sessionId);
            if (countQueryResult.getNumResults() != 1) {
                throw new Exception(countQueryResult.getErrorMsg());
            }

            long count = countQueryResult.first();
            AlignmentServiceModel.LongResponse longResponse = AlignmentServiceModel.LongResponse
                    .newBuilder()
                    .setValue(count)
                    .build();
            responseObserver.onNext(longResponse);
            responseObserver.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getAsSam(AlignmentServiceModel.AlignmentRequest request,
                         StreamObserver<AlignmentServiceModel.StringResponse> responseObserver) {
        // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
        Query query = GrpcServiceUtils.createQuery(request.getOptionsMap());
        QueryOptions queryOptions = GrpcServiceUtils.createQueryOptions(request.getOptionsMap());

        String studyIdStr = query.getString("study");
        String fileIdStr = query.getString("fileId");
        String sessionId = query.getString("sid");

        try (AlignmentIterator<SAMRecord> iterator =
                     alignmentStorageManager.iterator(studyIdStr, fileIdStr, query, queryOptions, sessionId, SAMRecord.class)) {
            while (iterator.hasNext()) {
                AlignmentServiceModel.StringResponse response =
                        AlignmentServiceModel.StringResponse
                                .newBuilder()
                                .setValue(iterator.next().getSAMString())
                                .build();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
