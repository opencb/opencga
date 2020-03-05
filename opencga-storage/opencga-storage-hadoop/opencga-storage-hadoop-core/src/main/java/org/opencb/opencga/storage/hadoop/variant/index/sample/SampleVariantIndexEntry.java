package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.Objects;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.VariantFileIndexConverter.MULTI_FILE_MASK;

public class SampleVariantIndexEntry implements Comparable<SampleVariantIndexEntry> {

    private final Variant variant;
    private final short fileIndex;

    public SampleVariantIndexEntry(Variant variant, short fileIndex) {
        this.variant = variant;
        this.fileIndex = fileIndex;
    }

    public Variant getVariant() {
        return variant;
    }

    public short getFileIndex() {
        return fileIndex;
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
