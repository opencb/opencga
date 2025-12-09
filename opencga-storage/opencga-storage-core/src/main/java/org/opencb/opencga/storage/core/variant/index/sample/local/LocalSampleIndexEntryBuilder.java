package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.opencga.storage.core.variant.index.sample.file.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeSet;

import static org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema.toRowKey;

public class LocalSampleIndexEntryBuilder extends SampleIndexEntryBuilder {
    public LocalSampleIndexEntryBuilder(int sampleId, String chromosome, int position, SampleIndexSchema schema,
            boolean orderedInput, boolean multiFileSample) {
        super(sampleId, chromosome, position, schema, orderedInput, multiFileSample);
    }

    public LocalSampleIndexEntryBuilder(int sampleId, String chromosome, int position, SampleIndexSchema schema,
            Map<String, TreeSet<SampleIndexVariant>> map) {
        super(sampleId, chromosome, position, schema, map);
    }

    public ByteBuffer build() {
        byte[] rk = toRowKey(sampleId, chromosome, position);

        return null;
    }

}
