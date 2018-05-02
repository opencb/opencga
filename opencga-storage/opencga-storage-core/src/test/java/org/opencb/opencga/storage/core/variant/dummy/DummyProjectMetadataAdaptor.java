package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.adaptors.ProjectMetadataAdaptor;

import java.util.Collections;
import java.util.concurrent.TimeoutException;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyProjectMetadataAdaptor implements ProjectMetadataAdaptor {
    private ProjectMetadata projectMetadata;

    @Override
    public long lockProject(long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        return 0;
    }

    @Override
    public void unLockProject(long lockId) {
    }

    @Override
    public synchronized QueryResult<ProjectMetadata> getProjectMetadata() {
        final QueryResult<ProjectMetadata> result = new QueryResult<>("");
        if (projectMetadata == null) {
            result.setResult(Collections.singletonList(new ProjectMetadata("hsapiens", "grch37", 1)));
        } else {
            result.setResult(Collections.singletonList(projectMetadata.copy()));
        }
        return result;
    }

    @Override
    public synchronized QueryResult updateProjectMetadata(ProjectMetadata projectMetadata) {
        this.projectMetadata = projectMetadata;
        return new QueryResult<>();
    }


}
