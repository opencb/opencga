package org.opencb.opencga.storage.mongodb.variant.index.sample;

import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;

import java.util.Collections;
import java.util.List;

public class MongoDBSampleIndexEntryWriter implements DataWriter<SampleIndexEntry> {

    private final MongoDBSampleIndexDBAdaptor dbAdaptor;
    private final int studyId;
    private final int schemaVersion;
    private MongoDBCollection collection;

    public MongoDBSampleIndexEntryWriter(MongoDBSampleIndexDBAdaptor dbAdaptor, int studyId, int schemaVersion) {
        this.dbAdaptor = dbAdaptor;
        this.studyId = studyId;
        this.schemaVersion = schemaVersion;
    }

    @Override
    public boolean open() {
        collection = dbAdaptor.createCollectionIfNeeded(studyId, schemaVersion);
        return true;
    }

    @Override
    public boolean write(SampleIndexEntry elem) {
        return write(Collections.singletonList(elem));
    }

    @Override
    public boolean write(List<SampleIndexEntry> list) {
        dbAdaptor.writeEntries(collection, list);
        return true;
    }
}
