package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;

import java.util.Comparator;
import java.util.Objects;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;

public class SampleVariantIndexEntry {

    private final Variant variant;
    private final String genotype;
    private final BitBuffer fileIndex;
    private final AnnotationIndexEntry annotationIndexEntry;
    private final Integer meCode;

    public SampleVariantIndexEntry(Variant variant, BitBuffer fileIndex) {
        this(variant, fileIndex, null, null);
    }

    public SampleVariantIndexEntry(Variant variant, BitBuffer fileIndex, String genotype, AnnotationIndexEntry annotationIndexEntry) {
        this(variant, fileIndex, genotype, annotationIndexEntry, null);
    }

    public SampleVariantIndexEntry(Variant variant, BitBuffer fileIndex, String genotype, AnnotationIndexEntry annotationIndexEntry,
                                   Integer meCode) {
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
        this.meCode = meCode;
    }

    public Variant getVariant() {
        return variant;
    }

    public BitBuffer getFileIndex() {
        return fileIndex;
    }

    public String getGenotype() {
        return genotype;
    }

    public Integer getMeCode() {
        return meCode;
    }

    public AnnotationIndexEntry getAnnotationIndexEntry() {
        return annotationIndexEntry;
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
        return fileIndex.equals(that.fileIndex) && Objects.equals(variant, that.variant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variant, fileIndex);
    }

    public static class SampleVariantIndexEntryComparator implements Comparator<SampleVariantIndexEntry> {

        private final SampleIndexSchema schema;

        public SampleVariantIndexEntryComparator(SampleIndexSchema schema) {
            this.schema = schema;
        }

        @Override
        public int compare(SampleVariantIndexEntry o1, SampleVariantIndexEntry o2) {
            int compare = INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(o1.variant, o2.variant);
            if (compare == 0) {
                if (schema.getFileIndex().isMultiFile(o1.fileIndex)) {
                    return -1;
                } else if (schema.getFileIndex().isMultiFile(o2.fileIndex)) {
                    return 1;
                } else {
                    return o1.fileIndex.compareTo(o2.fileIndex);
                }
            }
            return compare;
        }
    }
}
