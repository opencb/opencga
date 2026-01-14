package org.opencb.opencga.storage.mongodb.variant.index.sample.genotype;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexWriter;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoDBSampleIndexWriter extends SampleIndexWriter {
    private final MongoDBSampleIndexDBAdaptor dbAdaptor;
    private MongoDBCollection collection;

    public MongoDBSampleIndexWriter(MongoDBSampleIndexDBAdaptor dbAdaptor,
            VariantStorageMetadataManager metadataManager,
            int studyId, int fileId, List<Integer> sampleIds, SampleIndexSchema schema, ObjectMap options,
            VariantStorageEngine.SplitData splitData) {
        super(dbAdaptor, metadataManager, studyId, fileId, sampleIds, splitData, options, schema);
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
    protected void write(int remain) {
        if (buffer.size() <= remain) {
            return;
        }
        Iterator<Map.Entry<IndexChunk, Chunk>> iterator = buffer.entrySet().iterator();
        while (buffer.size() > remain && iterator.hasNext()) {
            Map.Entry<IndexChunk, Chunk> entry = iterator.next();
            Chunk chunk = entry.getValue();
            for (SampleIndexEntryBuilder builder : chunk) {
                if (builder.isEmpty()) {
                    continue;
                }
                dbAdaptor.writeEntry(collection, builder.buildEntry());
            }
            iterator.remove();
        }
    }
}
