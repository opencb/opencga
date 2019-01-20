package org.opencb.opencga.storage.mongodb.metadata;

import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.metadata.adaptors.CohortMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;

import java.util.Iterator;

/**
 * Created by jacobo on 20/01/19.
 */
public class MongoDBCohortMetadataDBAdaptor extends AbstractMongoDBAdaptor<CohortMetadata> implements CohortMetadataDBAdaptor {

    MongoDBCohortMetadataDBAdaptor(MongoDataStore db, String collectionName) {
        super(db, collectionName, CohortMetadata.class);
        createIdNameIndex();
    }

    @Override
    public CohortMetadata getCohortMetadata(int studyId, int cohortId, Long timeStamp) {
        return super.get(studyId, cohortId, null);
    }

    @Override
    public void updateCohortMetadata(int studyId, CohortMetadata cohort, Long timeStamp) {
        super.update(studyId, cohort.getId(), cohort);
    }

    @Override
    public Integer getCohortId(int studyId, String cohortName) {
        CohortMetadata obj = getId(buildQuery(studyId, cohortName));
        if (obj == null) {
            return null;
        } else {
            return obj.getId();
        }
    }

    @Override
    public Iterator<CohortMetadata> cohortIterator(int studyId) {
        return iterator(buildQuery(studyId), null);
    }
}
