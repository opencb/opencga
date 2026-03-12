package org.opencb.opencga.storage.core.variant.index.sample.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeSet;

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
        SampleIndexEntry entry = buildEntry();
        try {
            byte[] payload = MAPPER.writeValueAsBytes(entry);
            return ByteBuffer.wrap(payload);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to build local sample index entry", e);
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
}
