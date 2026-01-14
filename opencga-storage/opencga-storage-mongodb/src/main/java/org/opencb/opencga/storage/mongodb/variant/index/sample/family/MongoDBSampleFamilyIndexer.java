package org.opencb.opencga.storage.mongodb.variant.index.sample.family;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.family.SampleFamilyIndexer;

import java.util.List;

/**
 * Temporary Mongo-specific implementation that marks the entry points but does not yet perform any work.
 * Replace with the real pipeline when Mongo SampleIndex family indexing is implemented.
 */
public class MongoDBSampleFamilyIndexer extends SampleFamilyIndexer {

    public MongoDBSampleFamilyIndexer(SampleIndexDBAdaptor sampleIndexDBAdaptor) {
        super(sampleIndexDBAdaptor);
    }

    @Override
    protected void runBatch(String study, List<Trio> trios, ObjectMap options, int studyId, int version)
            throws StorageEngineException {
        throw new StorageEngineException("Mongo SampleIndex family build is not implemented yet");
    }
}

