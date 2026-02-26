package org.opencb.opencga.storage.core.variant.index.sample.genotype;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;

import java.util.List;
import java.util.Set;

/**
 * Writer to convert Variants into SampleIndexEntries and write them.
 *
 * Used to populate the sample index from variants during variant indexing.
 * Can be used to rewrite the sample index from existing variants.
 */
public class SampleIndexVariantWriter implements DataWriter<Variant> {

    private final SampleGenotypeIndexerTask converter;
    private final SampleIndexEntryWriter writer;

    public SampleIndexVariantWriter(SampleGenotypeIndexerTask converter, SampleIndexEntryWriter writer) {
        this.converter = converter;
        this.writer = writer;
    }

    public int getSampleIndexVersion() {
        return converter.getSampleIndexVersion();
    }

    public Set<String> getLoadedGenotypes() {
        return converter.getLoadedGenotypes();
    }

    @Override
    public boolean write(List<Variant> list) {
        List<SampleIndexEntry> entries = converter.apply(list);
        if (!entries.isEmpty()) {
            writer.write(entries);
        }
        return true;
    }

    @Override
    public boolean open() {
        return writer.open();
    }

    @Override
    public boolean close() {
        return writer.close();
    }

    @Override
    public boolean pre() {
        try {
            converter.pre();
            return writer.pre();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean post() {
        try {
            List<SampleIndexEntry> drained = converter.drain();
            if (!drained.isEmpty()) {
                writer.write(drained);
            }
            converter.post();
            return writer.post();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
