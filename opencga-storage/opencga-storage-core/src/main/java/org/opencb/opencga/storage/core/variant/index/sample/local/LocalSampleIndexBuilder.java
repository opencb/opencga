package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexBuilder;

import java.util.List;

public class LocalSampleIndexBuilder extends SampleIndexBuilder {
    public LocalSampleIndexBuilder(LocalSampleIndexDBAdaptor sampleIndexDBAdaptor, String study) {
        super(sampleIndexDBAdaptor, study);
    }

    @Override
    protected void buildSampleIndexBatch(int studyId, List<Integer> sampleIds, ObjectMap options) throws StorageEngineException {



    }
}
