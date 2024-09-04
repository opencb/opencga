package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;

public class SampleVariantIndexEntry {

    private final Variant variant;
    private final String genotype;
    private final List<BitBuffer> filesIndex;
    private final List<ByteBuffer> fileData;
    private final AnnotationIndexEntry annotationIndexEntry;
    private final Integer meCode;
    private final Byte parentsCode;

    public SampleVariantIndexEntry(Variant variant, BitBuffer fileIndex, ByteBuffer fileData) {
        this(variant, Collections.singletonList(fileIndex),
                fileData == null ? Collections.emptyList() : Collections.singletonList(fileData),
                null, null, null, null);
    }

    public SampleVariantIndexEntry(Variant variant, List<BitBuffer> filesIndex,  List<ByteBuffer> fileData, String genotype,
                                   AnnotationIndexEntry annotationIndexEntry, Byte parentsCode, Integer meCode) {
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
        this.filesIndex = filesIndex;
        this.fileData = fileData;
        this.genotype = genotype;
        this.annotationIndexEntry = annotationIndexEntry;
        this.meCode = meCode;
        this.parentsCode = parentsCode;
    }

    public Variant getVariant() {
        return variant;
    }

    @Deprecated
    public BitBuffer getFileIndex() {
        return filesIndex == null ? null : filesIndex.get(0);
    }

    public List<BitBuffer> getFilesIndex() {
        return filesIndex;
    }

    public List<ByteBuffer> getFileData() {
        return fileData;
    }

    public int getFileDataIndexBytes() {
        return fileData == null ? 0 : fileData.stream().mapToInt(ByteBuffer::limit).sum();
    }

    public String getGenotype() {
        return genotype;
    }

    public Integer getMeCode() {
        return meCode;
    }

    public Byte getParentsCode() {
        return parentsCode;
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
        return filesIndex.equals(that.filesIndex) && Objects.equals(variant, that.variant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variant, filesIndex, fileData);
    }

    public String toString(SampleIndexSchema schema) {
        return toString(schema, "\n");
    }

    public String toString(SampleIndexSchema schema, String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getVariant());
        sb.append(separator).append("gt: ")
                .append(this.getGenotype());
        sb.append(separator).append("file: ")
                .append(this.getFilesIndex());
        sb.append(separator).append("fileData: ");
        if (getFileData() == null) {
            sb.append("null");
        } else {
            sb.append("[");
            for (ByteBuffer fileDatum : getFileData()) {
                if (fileDatum == null) {
                    sb.append("null");
                } else {
                    sb.append(Bytes.toStringBinary(fileDatum));
                }
                sb.append(" , ");
            }
            sb.append("]");
        }
        sb.append(separator).append("me: ")
                .append(this.getMeCode());
        sb.append(separator).append("parents: ")
                .append(this.parentsCode);

        if (annotationIndexEntry != null) {
            annotationIndexEntry.toString(schema, separator, sb);
        }
        return sb.toString();
    }

    public static class SampleVariantIndexEntryComparator implements Comparator<SampleVariantIndexEntry> {

        private final SampleIndexSchema schema;

        public SampleVariantIndexEntryComparator(SampleIndexSchema schema) {
            this.schema = schema;
        }

        @Override
        public int compare(SampleVariantIndexEntry o1, SampleVariantIndexEntry o2) {
            int compare = INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(o1.variant, o2.variant);
            if (compare != 0) {
                return compare;
            }
            if (schema.getFileIndex().isMultiFile(o1.getFileIndex())) {
                return -1;
            } else if (schema.getFileIndex().isMultiFile(o2.getFileIndex())) {
                return 1;
            } else {
                int filePosition1 = schema.getFileIndex().getFilePositionIndex().read(o1.getFileIndex());
                int filePosition2 = schema.getFileIndex().getFilePositionIndex().read(o2.getFileIndex());
                compare = Integer.compare(filePosition1, filePosition2);
                if (compare != 0) {
                    return compare;
                }
                return o1.getFileIndex().compareTo(o2.getFileIndex());
            }
        }
    }
}
