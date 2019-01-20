package org.opencb.opencga.storage.mongodb.metadata;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.metadata.adaptors.*;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

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
    public ProjectMetadataAdaptor buildProjectMetadataDBAdaptor() {
        return new MongoDBProjectMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_PROJECT.key(),
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_PROJECT.defaultValue()
                ));
    }

    @Override
    public StudyMetadataDBAdaptor buildStudyConfigurationDBAdaptor() {
        return new MongoDBStudyMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_STUDIES.key(),
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_STUDIES.defaultValue()
                ));
    }

    @Override
    public SampleMetadataDBAdaptor buildSampleMetadataDBAdaptor() {
        return new MongoDBSampleMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_SAMPLES.key(),
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_SAMPLES.defaultValue()
        ));
    }

    @Override
    public CohortMetadataDBAdaptor buildCohortMetadataDBAdaptor() {
        return new MongoDBCohortMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_COHORTS.key(),
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_COHORTS.defaultValue()
        ));
    }

    @Override
    public TaskMetadataDBAdaptor buildTaskDBAdaptor() {
        return new MongoDBTaskMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_TASKS.key(),
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_TASKS.defaultValue()
        ));
    }

    @Override
    public FileMetadataDBAdaptor buildFileMetadataDBAdaptor() {
        return new MongoDBFileMetadataDBAdaptor(db, options.getString(
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_FILES.key(),
                MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_FILES.defaultValue()
        ));
    }
}
