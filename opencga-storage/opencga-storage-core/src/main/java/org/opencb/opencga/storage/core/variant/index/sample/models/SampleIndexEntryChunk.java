package org.opencb.opencga.storage.core.variant.index.sample.models;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.util.Objects;

public class SampleIndexEntryChunk {
    private final String chromosome;
    private final Integer batchStart;

    public SampleIndexEntryChunk(Variant variant) {
        this.chromosome = variant.getChromosome();
        this.batchStart = SampleIndexSchema.getChunkStart(variant.getStart());
    }

    public SampleIndexEntryChunk(String chromosome, Integer batchStart) {
        this.chromosome = chromosome;
        this.batchStart = batchStart;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SampleIndexEntryChunk)) {
            return false;
        }
        SampleIndexEntryChunk that = (SampleIndexEntryChunk) o;
        return Objects.equals(chromosome, that.chromosome)
                && Objects.equals(batchStart, that.batchStart);
    }

    public String getChromosome() {
        return chromosome;
    }

    public Integer getBatchStart() {
        return batchStart;
    }

    public Region toRegion() {
        return SampleIndexSchema.getChunkRegion(chromosome, batchStart, batchStart);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chromosome, batchStart);
    }

    @Override
    public String toString() {
        return chromosome + ":" + batchStart;
    }
}
