package org.opencb.opencga.storage.core.metadata.adaptors;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStorageMetadataDBAdaptorFactory {

    VariantFileMetadataDBAdaptor buildVariantFileMetadataDBAdaptor();

    ProjectMetadataAdaptor buildProjectMetadataDBAdaptor();

    StudyMetadataDBAdaptor buildStudyConfigurationDBAdaptor();

}
