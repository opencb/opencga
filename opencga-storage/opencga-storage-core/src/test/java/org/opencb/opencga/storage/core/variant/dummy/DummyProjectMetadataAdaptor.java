package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.ProjectMetadataAdaptor;

import java.util.Collections;
import java.util.concurrent.TimeoutException;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyProjectMetadataAdaptor extends ProjectMetadataAdaptor {
    private ProjectMetadata projectMetadata;

    @Override
    protected long lockProject(long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        return 0;
    }

    @Override
    protected void unLockProject(long lockId) {
    }

    public synchronized QueryResult<ProjectMetadata> getProjectMetadata() {
        final QueryResult<ProjectMetadata> result = new QueryResult<>("");
        if (projectMetadata == null) {
            result.setResult(Collections.singletonList(new ProjectMetadata("hsapiens", "grch37", 1)));
        } else {
            result.setResult(Collections.singletonList(projectMetadata.copy()));
        }
        return result;
    }

    public synchronized QueryResult updateProjectMetadata(ProjectMetadata projectMetadata) {
        this.projectMetadata = projectMetadata;
        return new QueryResult<>();
    }


}
