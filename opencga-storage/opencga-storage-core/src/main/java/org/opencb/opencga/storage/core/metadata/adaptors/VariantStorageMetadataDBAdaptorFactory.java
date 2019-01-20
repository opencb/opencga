package org.opencb.opencga.storage.core.metadata.adaptors;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStorageMetadataDBAdaptorFactory {

    FileMetadataDBAdaptor buildFileMetadataDBAdaptor();

    ProjectMetadataAdaptor buildProjectMetadataDBAdaptor();

    StudyMetadataDBAdaptor buildStudyConfigurationDBAdaptor();

    SampleMetadataDBAdaptor buildSampleMetadataDBAdaptor();

    CohortMetadataDBAdaptor buildCohortMetadataDBAdaptor();

    TaskMetadataDBAdaptor buildTaskDBAdaptor();
}
