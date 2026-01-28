package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryWriter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MongoDBSampleIndexEntryWriter extends SampleIndexEntryWriter {
    private final MongoDBSampleIndexDBAdaptor dbAdaptor;
    private MongoDBCollection collection;
    private int writtenEntries = 0;
    private Logger logger = LoggerFactory.getLogger(MongoDBSampleIndexEntryWriter.class);

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
        logger.info("Written {} sample index entries for study '{}' ({}), schema version {}",
                writtenEntries, dbAdaptor.getMetadataManager().getStudyName(studyId), studyId, schema.getVersion());
        return super.post();
    }

    @Override
    public boolean write(List<SampleIndexEntry> list) {
        if (!list.isEmpty()) {
            dbAdaptor.writeEntries(collection, list);
            writtenEntries += list.size();
        }
        return true;
    }

}
