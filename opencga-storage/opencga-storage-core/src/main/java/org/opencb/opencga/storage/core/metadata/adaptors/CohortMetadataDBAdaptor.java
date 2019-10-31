package org.opencb.opencga.storage.core.metadata.adaptors;

import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.Locked;

import java.util.Iterator;

/**
 * Created by jacobo on 20/01/19.
 */
public interface CohortMetadataDBAdaptor {

    CohortMetadata getCohortMetadata(int studyId, int cohortId, Long timeStamp);

    void updateCohortMetadata(int studyId, CohortMetadata cohort, Long timeStamp);

    Integer getCohortId(int studyId, String cohortName);

    Iterator<CohortMetadata> cohortIterator(int studyId);

    void removeCohort(int studyId, int cohortId);

    Locked lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException;
}
