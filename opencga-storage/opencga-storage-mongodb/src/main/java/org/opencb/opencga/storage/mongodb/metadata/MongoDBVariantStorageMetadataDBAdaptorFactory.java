package org.opencb.opencga.storage.mongodb.metadata;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.metadata.adaptors.*;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStorageMetadataDBAdaptorFactory implements VariantStorageMetadataDBAdaptorFactory {

    private final MongoDataStore db;
    private final ObjectMap options;

    public MongoDBVariantStorageMetadataDBAdaptorFactory(MongoDataStore db, ObjectMap options) {
        this.db = db;
        this.options = options;
    }

    @Override
    public ObjectMap getConfiguration() {
        return options;
    }

    @Override
    public ProjectMetadataAdaptor buildProjectMetadataDBAdaptor() {
        return new MongoDBProjectMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageOptions.COLLECTION_PROJECT.key(),
                MongoDBVariantStorageOptions.COLLECTION_PROJECT.defaultValue()
                ));
    }

    @Override
    public StudyMetadataDBAdaptor buildStudyMetadataDBAdaptor() {
        return new MongoDBStudyMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageOptions.COLLECTION_STUDIES.key(),
                MongoDBVariantStorageOptions.COLLECTION_STUDIES.defaultValue()
                ));
    }

    @Override
    public SampleMetadataDBAdaptor buildSampleMetadataDBAdaptor() {
        return new MongoDBSampleMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageOptions.COLLECTION_SAMPLES.key(),
                MongoDBVariantStorageOptions.COLLECTION_SAMPLES.defaultValue()
        ));
    }

    @Override
    public CohortMetadataDBAdaptor buildCohortMetadataDBAdaptor() {
        return new MongoDBCohortMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageOptions.COLLECTION_COHORTS.key(),
                MongoDBVariantStorageOptions.COLLECTION_COHORTS.defaultValue()
        ));
    }

    @Override
    public TaskMetadataDBAdaptor buildTaskDBAdaptor() {
        return new MongoDBTaskMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageOptions.COLLECTION_TASKS.key(),
                MongoDBVariantStorageOptions.COLLECTION_TASKS.defaultValue()
        ));
    }

    @Override
    public FileMetadataDBAdaptor buildFileMetadataDBAdaptor() {
        return new MongoDBFileMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageOptions.COLLECTION_FILES.key(),
                MongoDBVariantStorageOptions.COLLECTION_FILES.defaultValue()
        ), options.getString(
                MongoDBVariantStorageOptions.COLLECTION_STUDIES.key(),
                MongoDBVariantStorageOptions.COLLECTION_STUDIES.defaultValue()
        ));
    }
}
