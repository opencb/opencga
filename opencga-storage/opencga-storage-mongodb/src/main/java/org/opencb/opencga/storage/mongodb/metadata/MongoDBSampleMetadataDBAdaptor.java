package org.opencb.opencga.storage.mongodb.metadata;

import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.metadata.adaptors.SampleMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;

import java.util.Iterator;

/**
 * Created by jacobo on 20/01/19.
 */
public class MongoDBSampleMetadataDBAdaptor extends AbstractMongoDBAdaptor<SampleMetadata>
        implements SampleMetadataDBAdaptor {

    public MongoDBSampleMetadataDBAdaptor(MongoDataStore db, String collectionName) {
        super(db, collectionName, SampleMetadata.class);
        createIdNameIndex();
    }

    @Override
    public SampleMetadata getSampleMetadata(int studyId, int sampleId, Long timeStamp) {
        return get(studyId, sampleId, null);
    }

    @Override
    public void updateSampleMetadata(int studyId, SampleMetadata sample, Long timeStamp) {
        sample.setStudyId(studyId);
        update(studyId, sample.getId(), sample);
    }

    @Override
    public Iterator<SampleMetadata> sampleMetadataIterator(int studyId) {
        return iterator(buildQuery(studyId), null);
    }

    @Override
    public Integer getSampleId(int studyId, String sampleName) {
        SampleMetadata obj = getId(buildQuery(studyId, sampleName));
        if (obj == null) {
            return null;
        } else {
            return obj.getId();
        }
    }

}
