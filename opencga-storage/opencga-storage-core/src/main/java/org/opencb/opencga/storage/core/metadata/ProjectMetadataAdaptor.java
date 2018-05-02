package org.opencb.opencga.storage.core.metadata;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class ProjectMetadataAdaptor implements AutoCloseable {

    protected abstract long lockProject(long lockDuration, long timeout)
            throws InterruptedException, TimeoutException, StorageEngineException;

    protected abstract void unLockProject(long lockId) throws StorageEngineException;

    protected abstract QueryResult<ProjectMetadata> getProjectMetadata();

    protected abstract QueryResult updateProjectMetadata(ProjectMetadata projectMetadata);

    @Override
    public void close() throws IOException {
    }
}
