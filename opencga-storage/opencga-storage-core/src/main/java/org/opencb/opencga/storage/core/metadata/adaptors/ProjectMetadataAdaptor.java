package org.opencb.opencga.storage.core.metadata.adaptors;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ProjectMetadataAdaptor extends AutoCloseable {

    long lockProject(long lockDuration, long timeout)
            throws InterruptedException, TimeoutException, StorageEngineException;

    void unLockProject(long lockId) throws StorageEngineException;

    QueryResult<ProjectMetadata> getProjectMetadata();

    QueryResult updateProjectMetadata(ProjectMetadata projectMetadata, boolean updateCounters);

    @Override
    default void close() throws IOException {
    }

    int generateId(StudyConfiguration studyConfiguration, String idType) throws StorageEngineException;
}
