package org.opencb.opencga.storage.core.metadata.adaptors;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;

import java.io.IOException;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ProjectMetadataAdaptor extends AutoCloseable {

    default Lock lockProject(long lockDuration, long timeout)
            throws StorageEngineException {
        return lockProject(lockDuration, timeout, null);
    }

    Lock lockProject(long lockDuration, long timeout, String lockName)
            throws StorageEngineException;

    DataResult<ProjectMetadata> getProjectMetadata();

    DataResult updateProjectMetadata(ProjectMetadata projectMetadata, boolean updateCounters);

    @Override
    default void close() throws IOException {
    }

    int generateId(Integer studyId, String idType) throws StorageEngineException;

    boolean exists();
}
