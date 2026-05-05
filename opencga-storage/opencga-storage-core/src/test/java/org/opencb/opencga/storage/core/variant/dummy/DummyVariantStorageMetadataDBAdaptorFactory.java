package org.opencb.opencga.storage.core.variant.dummy;

import org.apache.commons.io.FileUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.adaptors.*;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by jacobo on 20/01/19.
 */
public class DummyVariantStorageMetadataDBAdaptorFactory implements VariantStorageMetadataDBAdaptorFactory {

    public DummyVariantStorageMetadataDBAdaptorFactory(String dbName) {
        this(false);
        if (dbName != null) {
            if (DummyVariantStorageEngine.DBNAME == null) {
                DummyVariantStorageEngine.DBNAME = dbName;
                DummyVariantStorageEngine.SAMPLE_INDEX_PATH = "target/test-data/dummy-variant-storage-engine/sample_index/" + dbName;
                if (Paths.get(DummyVariantStorageEngine.SAMPLE_INDEX_PATH).toFile().exists()) {
                    // Clear previous sample index
                    try {
                        System.err.println("Clearing previous sample index in " + DummyVariantStorageEngine.SAMPLE_INDEX_PATH);
                        FileUtils.deleteDirectory(Paths.get(DummyVariantStorageEngine.SAMPLE_INDEX_PATH).toFile());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else if (!DummyVariantStorageEngine.DBNAME.equals(dbName)) {
                throw new IllegalStateException("DummyVariantStorageMetadataDBAdaptor can't work with multiple dbNames at the same time. "
                        + "Already configured for '" + DummyVariantStorageEngine.DBNAME + "'. and requested for '" + dbName + "'");
            }
        }
    }

    public DummyVariantStorageMetadataDBAdaptorFactory() {
        this(false);
    }

    public DummyVariantStorageMetadataDBAdaptorFactory(boolean clear) {
        if (clear) {
            DummyVariantStorageEngine.clear();
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

}
