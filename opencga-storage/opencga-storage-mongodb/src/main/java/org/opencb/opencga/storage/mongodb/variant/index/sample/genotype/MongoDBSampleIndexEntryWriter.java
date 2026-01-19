package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryWriter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;

import java.util.List;

public class MongoDBSampleIndexEntryWriter extends SampleIndexEntryWriter {
    private final MongoDBSampleIndexDBAdaptor dbAdaptor;
    private MongoDBCollection collection;

    public MongoDBSampleIndexEntryWriter(MongoDBSampleIndexDBAdaptor dbAdaptor, int studyId, SampleIndexSchema schema) {
        super(dbAdaptor, studyId, schema);
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    public boolean open() {
        super.open();
        collection = dbAdaptor.createCollectionIfNeeded(studyId, schema.getVersion());
        return true;
    }

    @Override
    public boolean post() {
        return super.post();
    }

    @Override
    public boolean write(List<SampleIndexEntry> list) {
        dbAdaptor.writeEntries(collection, list);
        return true;
    }

}
