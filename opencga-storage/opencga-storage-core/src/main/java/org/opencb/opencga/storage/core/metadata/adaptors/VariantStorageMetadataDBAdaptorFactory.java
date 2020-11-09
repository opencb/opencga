package org.opencb.opencga.storage.core.metadata.adaptors;

import org.opencb.commons.datastore.core.ObjectMap;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStorageMetadataDBAdaptorFactory {

    ObjectMap getConfiguration();

    FileMetadataDBAdaptor buildFileMetadataDBAdaptor();

    ProjectMetadataAdaptor buildProjectMetadataDBAdaptor();

    StudyMetadataDBAdaptor buildStudyMetadataDBAdaptor();

    SampleMetadataDBAdaptor buildSampleMetadataDBAdaptor();

    CohortMetadataDBAdaptor buildCohortMetadataDBAdaptor();

    TaskMetadataDBAdaptor buildTaskDBAdaptor();
}
