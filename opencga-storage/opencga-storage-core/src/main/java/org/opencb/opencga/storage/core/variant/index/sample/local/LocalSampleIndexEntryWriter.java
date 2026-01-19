package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryWriter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.List;

public class LocalSampleIndexEntryWriter extends SampleIndexEntryWriter {
    private final LocalSampleIndexDBAdaptor dbAdaptor;

    public LocalSampleIndexEntryWriter(LocalSampleIndexDBAdaptor dbAdaptor, int studyId,
                                       SampleIndexSchema schema) {
        super(dbAdaptor, studyId, schema);
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    public boolean write(List<SampleIndexEntry> list) {
        for (SampleIndexEntry sampleIndexEntry : list) {
            try {
                dbAdaptor.writeEntry(studyId, schema.getVersion(), sampleIndexEntry);
            } catch (StorageEngineException e) {
                throw new RuntimeException("Error writing sample index entry for sample " + sampleIndexEntry.getSampleId(), e);
            }
        }
        return true;
    }

}
