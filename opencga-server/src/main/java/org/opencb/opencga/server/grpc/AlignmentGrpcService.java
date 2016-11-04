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
public class AlignmentGrpcService extends AlignmentServiceGrpc.AlignmentServiceImplBase {

    private GenericGrpcService genericGrpcService;

    public AlignmentGrpcService(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration) {
//        super(catalogConfiguration, storageConfiguration);

        genericGrpcService = new GenericGrpcService(catalogConfiguration, storageConfiguration);
    }

    @Override
    public void count(GenericAlignmentServiceModel.Request request, StreamObserver<ServiceTypesModel.LongResponse> responseObserver) {

    }

    @Override
    public void distinct(GenericAlignmentServiceModel.Request request, StreamObserver<ServiceTypesModel.StringArrayResponse> responseObserver) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void get(GenericAlignmentServiceModel.Request request, StreamObserver<Reads.ReadAlignment> responseObserver) {

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


}
