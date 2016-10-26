package org.opencb.opencga.server.grpc;

import ga4gh.Reads;
import io.grpc.stub.StreamObserver;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.adaptors.DefaultAlignmentDBAdaptor;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by pfurio on 26/10/16.
 */
public class AlignmentGrpcService extends GenericGrpcService implements AlignmentServiceGrpc.AlignmentService {

    public AlignmentGrpcService(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration) {
        super(catalogConfiguration, storageConfiguration);
    }

    @Override
    public void count(GenericAlignmentServiceModel.Request request, StreamObserver<ServiceTypesModel.LongResponse> responseObserver) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void distinct(GenericAlignmentServiceModel.Request request, StreamObserver<ServiceTypesModel.StringArrayResponse> responseObserver) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void get(GenericAlignmentServiceModel.Request request, StreamObserver<Reads.ReadAlignment> responseObserver) {
        try {
            // Creating the datastore Query and QueryOptions objects from the gRPC request Map of Strings
//            Query query = createQuery(request);
//            QueryOptions queryOptions = createQueryOptions(request);
            System.out.println("HELLOOOOO");
            Query query = new Query();
            QueryOptions queryOptions = new QueryOptions();

            Path path = Paths.get(request.getQuery().get("path"));

            AlignmentDBAdaptor alignmentDBAdaptor = new DefaultAlignmentDBAdaptor(path);

            Iterator iterator = alignmentDBAdaptor.iterator(query, queryOptions);
            while (iterator.hasNext()) {
                responseObserver.onNext((Reads.ReadAlignment) iterator.next());
            }
            responseObserver.onCompleted();
//            alignmentDBAdaptor.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void groupBy(GenericAlignmentServiceModel.Request request, StreamObserver<ServiceTypesModel.GroupResponse> responseObserver) {
        throw new UnsupportedOperationException();
    }
}
