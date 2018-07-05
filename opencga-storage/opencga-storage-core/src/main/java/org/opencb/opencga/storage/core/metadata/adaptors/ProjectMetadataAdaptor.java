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

    QueryResult updateProjectMetadata(ProjectMetadata projectMetadata);

    @Override
    default void close() throws IOException {
    }

    default int generateId(StudyConfiguration studyConfiguration, String idType) throws StorageEngineException {
        Integer id;
        try {
            long lock = lockProject(1000, 10000);
            ProjectMetadata projectMetadata = getProjectMetadata().first();
            id = projectMetadata.getCounters().compute(idType + (studyConfiguration == null ? "" : ('_' + studyConfiguration.getStudyId())),
                    (key, value) -> value == null ? 1 : value + 1);

            updateProjectMetadata(projectMetadata);
            unLockProject(lock);

        } catch (TimeoutException | InterruptedException e) {
            throw new StorageEngineException("Error generating new ID for " + idType, e);
        }
        return id;
    }
}
