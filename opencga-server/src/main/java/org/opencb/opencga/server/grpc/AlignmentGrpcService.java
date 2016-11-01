package org.opencb.opencga.server.grpc;

import ga4gh.Reads;
import io.grpc.stub.StreamObserver;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.local.DefaultAlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 26/10/16.
 */
public class AlignmentGrpcService extends GenericGrpcService implements AlignmentServiceGrpc.AlignmentService {

    public AlignmentGrpcService(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration) {
        super(catalogConfiguration, storageConfiguration);
    }

    @Override
    public void count(GenericAlignmentServiceModel.Request request, StreamObserver<ServiceTypesModel.LongResponse> responseObserver) {
        try {
            // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
            Query query = createQuery(request);
            QueryOptions queryOptions = createQueryOptions(request);
            Path path = getPath(query);

            AlignmentDBAdaptor alignmentDBAdaptor = new DefaultAlignmentDBAdaptor();

            long count = alignmentDBAdaptor.count(path.toString(), query, queryOptions);
            ServiceTypesModel.LongResponse longResponse = ServiceTypesModel.LongResponse.newBuilder().setValue(count).build();
            responseObserver.onNext(longResponse);
            responseObserver.onCompleted();
//            alignmentDBAdaptor.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void distinct(GenericAlignmentServiceModel.Request request, StreamObserver<ServiceTypesModel.StringArrayResponse> responseObserver) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void get(GenericAlignmentServiceModel.Request request, StreamObserver<Reads.ReadAlignment> responseObserver) {
        try {
            // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
            Query query = createQuery(request);

            String fileIdStr = query.getString("fileId");
            String sessionId = query.getString("sid");

            String userId = catalogManager.getUserManager().getId(sessionId);
            Long fileId = catalogManager.getFileManager().getId(userId, fileIdStr);

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key());
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileId, queryOptions, sessionId);

            if (fileQueryResult != null && fileQueryResult.getNumResults() != 1) {
                // This should never happen
                throw new CatalogException("Critical error: File " + fileId + " could not be found in catalog.");
            }
            Path path = Paths.get(fileQueryResult.first().getUri());

            AlignmentDBAdaptor alignmentDBAdaptor = new DefaultAlignmentDBAdaptor();

            queryOptions = createQueryOptions(request);
            try (AlignmentIterator iterator = alignmentDBAdaptor.iterator(path.toString(), query, queryOptions)) {
                while (iterator.hasNext()) {
                    responseObserver.onNext((Reads.ReadAlignment) iterator.next());
                }
                responseObserver.onCompleted();
            }
//            alignmentDBAdaptor.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void groupBy(GenericAlignmentServiceModel.Request request, StreamObserver<ServiceTypesModel.GroupResponse> responseObserver) {
        throw new UnsupportedOperationException();
    }

    // TODO: Temporal solution. We have to implement a general createQuery and createQueryOptions
    private Query createQuery(GenericAlignmentServiceModel.Request request) {
        Query query = new Query();
        for (String key : request.getQuery().keySet()) {
            if (request.getQuery().get(key) != null) {
                query.put(key, request.getQuery().get(key));
            }
        }
        return query;
    }

    private QueryOptions createQueryOptions(GenericAlignmentServiceModel.Request request) {
        QueryOptions queryOptions = new QueryOptions();
        for (String key : request.getOptions().keySet()) {
            if (request.getOptions().get(key) != null) {
                queryOptions.put(key, request.getOptions().get(key));
            }
        }
        return queryOptions;
    }

    /**
     * Obtain the path corresponding to the file id.
     *
     * @param query query.
     * @return the path corresponding to the file id.
     * @throws CatalogException
     * @throws IOException
     */
    private Path getPath(Query query) throws CatalogException {
        String fileIdStr = query.getString("fileId");
        String sessionId = query.getString("sid");

        String userId = catalogManager.getUserManager().getId(sessionId);
        Long fileId = catalogManager.getFileManager().getId(userId, fileIdStr);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.URI.key());
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileId, options, sessionId);

        if (fileQueryResult != null && fileQueryResult.getNumResults() != 1) {
            // This should never happen
            throw new CatalogException("Critical error: File " + fileId + " could not be found in catalog.");
        }

        return Paths.get(fileQueryResult.first().getUri());
    }

}
