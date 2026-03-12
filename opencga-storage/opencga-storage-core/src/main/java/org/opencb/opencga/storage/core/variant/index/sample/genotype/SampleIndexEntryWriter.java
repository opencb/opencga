package org.opencb.opencga.storage.core.variant.index.sample.genotype;

import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

public abstract class SampleIndexEntryWriter implements DataWriter<SampleIndexEntry> {

    protected final SampleIndexDBAdaptor dbAdaptor;
    protected final int studyId;
    protected final SampleIndexSchema schema;

    public SampleIndexEntryWriter(SampleIndexDBAdaptor dbAdaptor, int studyId, SampleIndexSchema schema) {
        this.dbAdaptor = dbAdaptor;
        this.studyId = studyId;
        this.schema = schema;
    }

}
