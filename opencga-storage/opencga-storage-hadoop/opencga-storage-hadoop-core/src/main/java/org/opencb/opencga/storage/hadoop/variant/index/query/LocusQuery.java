package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.REGION_COMPARATOR;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;

/**
 * Sample index queries based on position, aligned to SampleIndex chunks.
 */
public class LocusQuery {
    /**
     * Region aligned with sampleIndex chunks covering all locus from regions and variants.
     */
    private final Region chunkRegion;
    private final List<Region> regions;
    private final List<Variant> variants;

    public LocusQuery(Region chunkRegion) {
        this.chunkRegion = chunkRegion;
        regions = new ArrayList<>();
        variants = new ArrayList<>();
    }

    public LocusQuery(Region chunkRegion, List<Region> regions, List<Variant> variants) {
        this.chunkRegion = chunkRegion;
        this.regions = regions;
        this.variants = variants;
    }

    public static LocusQuery buildLocusQuery(Region r) {
        LocusQuery locusQuery = new LocusQuery(SampleIndexSchema.getChunkRegion(r));
        locusQuery.getRegions().add(r);
        return locusQuery;
    }

    public Region getChunkRegion() {
        return chunkRegion;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public List<Variant> getVariants() {
        return variants;
    }

    public boolean isEmpty() {
        return variants.isEmpty() && regions.isEmpty();
    }

    public void merge(LocusQuery other) {
        chunkRegion.setStart(Math.min(chunkRegion.getStart(), other.chunkRegion.getStart()));
        chunkRegion.setEnd(Math.max(chunkRegion.getEnd(), other.chunkRegion.getEnd()));

        regions.addAll(other.regions);
        regions.sort(REGION_COMPARATOR);

        variants.addAll(other.variants);
        variants.sort(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocusQuery that = (LocusQuery) o;
        return Objects.equals(chunkRegion, that.chunkRegion)
                && Objects.equals(regions, that.regions)
                && Objects.equals(variants, that.variants);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkRegion, regions, variants);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LocusQuery{");
        sb.append("chunkRegion=").append(chunkRegion);
        sb.append(", regions=").append(regions);
        sb.append(", variants=").append(variants);
        sb.append('}');
        return sb.toString();
    }
}
