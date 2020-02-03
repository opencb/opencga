package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.hadoop.conf.Configuration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.CohortMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.util.Iterator;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created by jacobo on 20/01/19.
 */
public class HBaseCohortMetadataDBAdaptor extends AbstractHBaseDBAdaptor implements CohortMetadataDBAdaptor {

    public HBaseCohortMetadataDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        super(hBaseManager, metaTableName, configuration);
    }

    @Override
    public CohortMetadata getCohortMetadata(int studyId, int cohortId, Long timeStamp) {
        return readValue(getCohortMetadataRowKey(studyId, cohortId), CohortMetadata.class, timeStamp);
    }

    @Override
    public void updateCohortMetadata(int studyId, CohortMetadata cohort, Long timeStamp) {
        putValue(getCohortNameIndexRowKey(studyId, cohort.getName()), HBaseVariantMetadataUtils.Type.INDEX, cohort.getId(), timeStamp);
        putValue(getCohortMetadataRowKey(studyId, cohort.getId()), HBaseVariantMetadataUtils.Type.COHORT, cohort, timeStamp);
    }

    @Override
    public Integer getCohortId(int studyId, String cohortName) {
        return readValue(getCohortNameIndexRowKey(studyId, cohortName), Integer.class, null);
    }

    @Override
    public Iterator<CohortMetadata> cohortIterator(int studyId) {
        return iterator(getCohortMetadataRowKeyPrefix(studyId), CohortMetadata.class, false);
    }

    @Override
    public void removeCohort(int studyId, int cohortId) {
        CohortMetadata cohort = getCohortMetadata(studyId, cohortId, null);
        deleteRow(getCohortNameIndexRowKey(studyId, cohort.getName()));
        deleteRow(getCohortMetadataRowKey(studyId, cohort.getId()));
    }

    @Override
    public Lock lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
        return lock(getCohortMetadataRowKey(studyId, id), lockDuration, timeout);
    }
}
