package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.index.sample.file.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.file.SampleIndexWriter;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LocalSampleIndexWriter extends SampleIndexWriter {
    private final LocalSampleIndexDBAdaptor dbAdaptor;

    public LocalSampleIndexWriter(LocalSampleIndexDBAdaptor dbAdaptor, VariantStorageMetadataManager metadataManager,
                                  int studyId, int fileId, List<Integer> sampleIds, VariantStorageEngine.SplitData splitData,
                                  ObjectMap options, SampleIndexSchema schema) {
        super(dbAdaptor, metadataManager, studyId, fileId, sampleIds, splitData, options, schema);
        this.dbAdaptor = dbAdaptor;
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
                try {
                    dbAdaptor.writeEntry(studyId, schema.getVersion(), builder.buildEntry());
                } catch (StorageEngineException e) {
                    throw new RuntimeException("Error writing sample index entry for chunk " + entry.getKey(), e);
                }
            }
            iterator.remove();
        }
    }
}
