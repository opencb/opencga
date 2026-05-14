package org.opencb.opencga.storage.mongodb.annotation;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexer;
import org.opencb.opencga.storage.core.variant.io.db.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.io.db.VariantMongoDBAnnotationDBWriter;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverter.ANNOT_ID;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.*;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantAnnotationManager extends DefaultVariantAnnotationManager {
    private final VariantMongoDBAdaptor mongoDbAdaptor;

    public MongoDBVariantAnnotationManager(VariantAnnotator annotator, VariantMongoDBAdaptor mongoDbAdaptor,
                                           IOConnectorProvider ioConnectorProvider, SampleAnnotationIndexer sampleIndexAnnotation) {
        super(annotator, mongoDbAdaptor, ioConnectorProvider, sampleIndexAnnotation);
        this.mongoDbAdaptor = mongoDbAdaptor;
    }

    @Override
    protected VariantAnnotationDBWriter newVariantAnnotationDBWriter(VariantDBAdaptor dbAdaptor, QueryOptions options) {
        return new VariantMongoDBAnnotationDBWriter(options, mongoDbAdaptor);
    }

    @Override
    public void saveAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException {
        // registerNewAnnotationSnapshot captures the state of current BEFORE bumping (the snapshot's
        // id == current.id pre-bump), then bumps current.id. The just-snapshotted id is what we
        // filter the data copy by, so only variants stamped under THIS generation get into the
        // saved collection — phantom intermediate ids (where no variant was ever stamped) produce
        // an empty saved collection, which is the correct outcome.
        AtomicInteger snapshotIdRef = new AtomicInteger();
        dbAdaptor.getMetadataManager().updateProjectMetadata(project -> {
            ProjectMetadata.VariantAnnotationMetadata snap =
                    registerNewAnnotationSnapshot(name, variantAnnotator, project);
            snapshotIdRef.set(snap.getId());
            return project;
        });
        int snapshotId = snapshotIdRef.get();

        String annotationCollectionName = mongoDbAdaptor.getAnnotationCollectionName(name);
        mongoDbAdaptor.getVariantsCollection()
                .aggregate(Arrays.asList(
                        match(eq(ANNOT_ID, snapshotId)),
                        project(include(
                                CHROMOSOME_FIELD,
                                START_FIELD,
                                REFERENCE_FIELD,
                                ALTERNATE_FIELD,
                                END_FIELD,
                                TYPE_FIELD,
                                SV_FIELD,
                                RELEASE_FIELD,
                                ANNOTATION_FIELD,
                                CUSTOM_ANNOTATION_FIELD)),
                        out(annotationCollectionName)), new QueryOptions());

    }

    @Override
    public void deleteAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException {
        mongoDbAdaptor.dropAnnotationCollection(name);

        dbAdaptor.getMetadataManager().updateProjectMetadata(project -> {
            removeAnnotationSnapshot(name, project);
            return project;
        });

    }
}
