package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;

import java.util.Objects;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.VariantFileIndexConverter.MULTI_FILE_MASK;

public class SampleVariantIndexEntry implements Comparable<SampleVariantIndexEntry> {

    private final Variant variant;
    private final String genotype;
    private final short fileIndex;
    private final AnnotationIndexEntry annotationIndexEntry;

    public SampleVariantIndexEntry(Variant variant, short fileIndex) {
        this(variant, fileIndex, null, null);
    }

    public SampleVariantIndexEntry(Variant variant, short fileIndex, String genotype, AnnotationIndexEntry annotationIndexEntry) {
        if (CollectionUtils.isEmpty(variant.getImpl().getStudies())) {
            this.variant = variant;
        } else {
            // Copy variant to allow GC discard the input variant if needed.
            this.variant = new Variant(new VariantAvro(
                    null, null,
                    variant.getChromosome(),
                    variant.getStart(),
                    variant.getEnd(),
                    variant.getReference(),
                    variant.getAlternate(),
                    null,
                    variant.getSv(),
                    variant.getLength(),
                    variant.getType(),
                    null, null));
        }
        this.fileIndex = fileIndex;
        this.genotype = genotype;
        this.annotationIndexEntry = annotationIndexEntry;
    }

    public Variant getVariant() {
        return variant;
    }

    public short getFileIndex() {
        return fileIndex;
    }

    public String getGenotype() {
        return genotype;
    }

    public AnnotationIndexEntry getAnnotationIndexEntry() {
        return annotationIndexEntry;
    }

    @Override
    public int compareTo(SampleVariantIndexEntry o) {
        int compare = INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(variant, o.variant);
        if (compare == 0) {
            if (IndexUtils.testIndexAny(fileIndex, MULTI_FILE_MASK)) {
                return -1;
            } else if (IndexUtils.testIndexAny(o.fileIndex, MULTI_FILE_MASK)) {
                return 1;
            } else {
                return Short.compare(fileIndex, o.fileIndex);
            }
        }
        return compare;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SampleVariantIndexEntry that = (SampleVariantIndexEntry) o;
        return fileIndex == that.fileIndex && Objects.equals(variant, that.variant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variant, fileIndex);
    }
}
