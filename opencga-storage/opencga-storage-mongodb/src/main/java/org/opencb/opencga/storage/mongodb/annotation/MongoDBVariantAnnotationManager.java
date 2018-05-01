package org.opencb.opencga.storage.mongodb.annotation;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.db.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.io.db.VariantMongoDBAnnotationDBWriter;

import java.util.Arrays;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Projections.include;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverter.ANNOT_ID_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.*;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantAnnotationManager extends DefaultVariantAnnotationManager {
    private final VariantMongoDBAdaptor mongoDbAdaptor;

    public MongoDBVariantAnnotationManager(VariantAnnotator annotator, VariantMongoDBAdaptor mongoDbAdaptor) {
        super(annotator, mongoDbAdaptor);
        this.mongoDbAdaptor = mongoDbAdaptor;
    }

    @Override
    protected VariantAnnotationDBWriter newVariantAnnotationDBWriter(VariantDBAdaptor dbAdaptor, QueryOptions options) {
        return new VariantMongoDBAnnotationDBWriter(options, mongoDbAdaptor);
    }

    @Override
    public void createAnnotationSnapshot(String name, ObjectMap options) throws StorageEngineException {
        // TODO: Check valid name
        // TODO: Check name doesn't exist already

        String annotationCollectionName = mongoDbAdaptor.getAnnotationCollectionName(name);
        mongoDbAdaptor.getVariantsCollection().aggregate(Arrays.asList(
                match(exists(ANNOTATION_FIELD + '.' + ANNOT_ID_FIELD)),
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
                out(annotationCollectionName)), null);
    }

    @Override
    public void deleteAnnotationSnapshot(String name, ObjectMap options) throws StorageEngineException {
        mongoDbAdaptor.dropAnnotationCollection(name);
    }
}
