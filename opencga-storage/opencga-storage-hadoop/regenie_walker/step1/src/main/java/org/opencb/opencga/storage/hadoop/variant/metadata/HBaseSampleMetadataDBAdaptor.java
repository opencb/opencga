package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.hadoop.conf.Configuration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.adaptors.SampleMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.util.Iterator;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created by jacobo on 20/01/19.
 */
public class HBaseSampleMetadataDBAdaptor extends AbstractHBaseDBAdaptor implements SampleMetadataDBAdaptor {

    public HBaseSampleMetadataDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        super(hBaseManager, metaTableName, configuration);
    }

    @Override
    public SampleMetadata getSampleMetadata(int studyId, int sampleId, Long timeStamp) {
        return readValue(getSampleMetadataRowKey(studyId, sampleId), SampleMetadata.class, timeStamp);
    }

    @Override
    public void updateSampleMetadata(int studyId, SampleMetadata sample, Long timeStamp) {
        putValue(getSampleNameIndexRowKey(studyId, sample.getName()), HBaseVariantMetadataUtils.Type.INDEX, sample.getId(), timeStamp);
        putValue(getSampleMetadataRowKey(studyId, sample.getId()), HBaseVariantMetadataUtils.Type.SAMPLE, sample, timeStamp);
    }

    @Override
    public Iterator<SampleMetadata> sampleMetadataIterator(int studyId) {
        return iterator(getSampleMetadataRowKeyPrefix(studyId), SampleMetadata.class, false);
    }

    @Override
    public Integer getSampleId(int studyId, String sampleName) {
        return readValue(getSampleNameIndexRowKey(studyId, sampleName), Integer.class, null);
    }

    @Override
    public Lock lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
        return lock(getSampleMetadataRowKey(studyId, id), lockDuration, timeout);
    }
}
