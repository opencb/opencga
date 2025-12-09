package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.List;

public class LocalSampleIndexBuilder extends SampleIndexBuilder {
    public LocalSampleIndexBuilder(LocalSampleIndexDBAdaptor sampleIndexDBAdaptor) {
        super(sampleIndexDBAdaptor);
    }

    @Override
    protected void runBatch(int studyId, SampleIndexSchema schema, List<Integer> sampleIds, ObjectMap options)
            throws StorageEngineException {

    }
}
