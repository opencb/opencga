package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.adaptors.*;

import java.nio.file.Path;

/**
 * Created by jacobo on 20/01/19.
 */
public class DummyVariantStorageMetadataDBAdaptorFactory implements VariantStorageMetadataDBAdaptorFactory {

    public DummyVariantStorageMetadataDBAdaptorFactory() {
        this(false);
    }

    public DummyVariantStorageMetadataDBAdaptorFactory(boolean clear) {
        if (clear) {
            clear();
        }
    }

    @Override
    public ObjectMap getConfiguration() {
        return new ObjectMap();
    }

    @Override
    public FileMetadataDBAdaptor buildFileMetadataDBAdaptor() {
        return new DummyFileMetadataDBAdaptor();
    }

    @Override
    public ProjectMetadataAdaptor buildProjectMetadataDBAdaptor() {
        return new DummyProjectMetadataAdaptor();
    }

    @Override
    public StudyMetadataDBAdaptor buildStudyMetadataDBAdaptor() {
        return new DummyStudyMetadataDBAdaptor();
    }

    @Override
    public SampleMetadataDBAdaptor buildSampleMetadataDBAdaptor() {
        return new DummyStudyMetadataDBAdaptor();
    }

    @Override
    public CohortMetadataDBAdaptor buildCohortMetadataDBAdaptor() {
        return new DummyStudyMetadataDBAdaptor();
    }

    @Override
    public TaskMetadataDBAdaptor buildTaskDBAdaptor() {
        return new DummyStudyMetadataDBAdaptor();
    }

    public static void clear() {
        DummyProjectMetadataAdaptor.clear();
        DummyStudyMetadataDBAdaptor.clear();
        DummyFileMetadataDBAdaptor.clear();
    }

    public static void writeAndClear(Path path) {
        DummyProjectMetadataAdaptor.writeAndClear(path);
        DummyStudyMetadataDBAdaptor.writeAndClear(path);
        DummyFileMetadataDBAdaptor.writeAndClear(path);
    }
}
