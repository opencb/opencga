package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitInputStream;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexEntryIterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.isGenotypeColumn;

/**
 * Created on 11/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexVariantBiConverter {

    private static final char STRING_SEPARATOR = ',';

    public static final int SEPARATOR_LENGTH = 1;
    public static final int INT24_LENGTH = 3;
    public static final byte BYTE_SEPARATOR = 0;

    private final SampleIndexSchema schema;

    public SampleIndexVariantBiConverter(SampleIndexSchema schema) {
        this.schema = schema;
    }

    public int expectedSize(Variant variant, boolean interVariantSeparator) {
        return expectedSize(variant.getReference(), getAlternate(variant), interVariantSeparator);
    }

    protected int expectedSize(String reference, String alternate, boolean interVariantSeparator) {
        if (AlleleSnvCodec.valid(reference, alternate)) {
            return INT24_LENGTH; // interVariantSeparator not needed when coding alleles
        } else {
            return INT24_LENGTH + reference.length() + SEPARATOR_LENGTH + alternate.length()
                    + (interVariantSeparator ? SEPARATOR_LENGTH : 0);
        }
    }

    public byte[] toBytesFromStrings(Collection<String> variantsStr) {
        List<Variant> variants = new ArrayList<>(variantsStr.size());
        for (String s : variantsStr) {
            variants.add(new Variant(s));
        }
        return toBytes(variants);
    }

    public byte[] toBytes(Collection<Variant> variants) {
        int size = 0;
        for (Variant variant : variants) {
            size += expectedSize(variant, true);
        }
        byte[] bytes = new byte[size];
        toBytes(variants, bytes, 0);
        return bytes;
    }

    protected int toBytes(Collection<Variant> variants, byte[] bytes, int offset) {
        int length = 0;
        for (Variant variant : variants) {
            length += toBytes(variant, bytes, offset + length, true);
        }
        return length;
    }

    public byte[] toBytes(Variant variant) {
        return toBytes(variant, false);
    }

    public byte[] toBytes(Variant variant, boolean interVariantSeparator) {
        String alternate = getAlternate(variant);
        byte[] bytes = new byte[expectedSize(variant.getReference(), alternate, interVariantSeparator)];
        toBytes(getRelativeStart(variant), variant.getReference(), alternate, bytes, 0, interVariantSeparator);
        return bytes;
    }

    public void toBytes(Variant variant, ByteArrayOutputStream stream) throws IOException {
        stream.write(toBytes(variant, true));
    }

    public int toBytes(Variant variant, byte[] bytes, int offset) {
        return toBytes(variant, bytes, offset, false);
    }

    public int toBytes(Variant variant, byte[] bytes, int offset, boolean interVariantSeparator) {
        return toBytes(getRelativeStart(variant), variant.getReference(), getAlternate(variant), bytes, offset, interVariantSeparator);
    }

    protected int toBytes(int relativeStart, String reference, String alternate, byte[] bytes, int offset, boolean interVariantSeparator) {
        if (AlleleSnvCodec.valid(reference, alternate)) {
            int length = append24bitInteger(relativeStart, bytes, offset);
            bytes[offset] |= AlleleSnvCodec.encode(reference, alternate);
            return length;
        } else {
            int length = 0;
            length += append24bitInteger(relativeStart, bytes, offset + length);
            length += appendString(reference, bytes, offset + length);
            length += appendSeparator(bytes, offset + length);
            length += appendString(alternate, bytes, offset + length);
            if (interVariantSeparator) {
                length += appendSeparator(bytes, offset + length);
            }
            return length;
        }
    }

    public Variant toVariant(String chromosome, int batchStart, byte[] bytes) {
        return toVariant(chromosome, batchStart, bytes, 0);
    }

    public Variant toVariant(String chromosome, int batchStart, byte[] bytes, int offset) {
        if (hasEncodedAlleles(bytes, offset)) {
            return toVariantEncodedAlleles(chromosome, batchStart, bytes, offset);
        } else {
            int referenceLength = readNextSeparator(bytes, offset + INT24_LENGTH);
            int alternateLength = readNextSeparator(bytes, offset + INT24_LENGTH + referenceLength + SEPARATOR_LENGTH);
            return toVariant(chromosome, batchStart, bytes, offset, referenceLength, alternateLength);
        }
    }

    public List<Variant> toVariants(Cell cell) {
        List<Variant> variants;
        if (isGenotypeColumn(cell)) {
            byte[] row = CellUtil.cloneRow(cell);
            String chromosome = SampleIndexSchema.chromosomeFromRowKey(row);
            int batchStart = SampleIndexSchema.batchStartFromRowKey(row);
            variants = toVariants(chromosome, batchStart, cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        } else {
            variants = Collections.emptyList();
        }
        return variants;
    }

    public List<Variant> toVariants(String chromosome, int batchStart, byte[] bytes, int offset, int length) {
        // Create dummy entry
        SampleIndexEntry entry = new SampleIndexEntry(0, chromosome, batchStart);
        SampleIndexEntry.SampleIndexGtEntry gtEntry = entry.getGtEntry("0/1");
        gtEntry.setVariants(bytes, offset, length);
        SampleIndexEntryIterator it = toVariantsIterator(gtEntry);
        List<Variant> variants = new ArrayList<>(it.getApproxSize());
        it.forEachRemaining(variants::add);
        return variants;
    }

    public SampleIndexEntryIterator toVariantsIterator(SampleIndexEntry entry, String gt, boolean onlyCount) {
        if (onlyCount) {
            return toVariantsCountIterator(entry, gt);
        } else {
            return toVariantsIterator(entry, gt);
        }
    }

    public SampleIndexEntryIterator toVariantsIterator(SampleIndexEntry.SampleIndexGtEntry entry, boolean onlyCount) {
        if (onlyCount) {
            return toVariantsCountIterator(entry);
        } else {
            return toVariantsIterator(entry);
        }
    }

    public SampleIndexEntryIterator toVariantsIterator(SampleIndexEntry entry, String gt) {
        return toVariantsIterator(entry.getGts().get(gt));
    }

    public SampleIndexEntryIterator toVariantsIterator(SampleIndexEntry.SampleIndexGtEntry gtEntry) {
        if (gtEntry == null || gtEntry.getVariantsLength() <= 0) {
            return EmptySampleIndexEntryIterator.emptyIterator();
        } else {
            return new ByteSampleIndexGtEntryIterator(
                    gtEntry.getEntry().getChromosome(),
                    gtEntry.getEntry().getBatchStart(), gtEntry, schema);
        }
    }

    public SampleIndexEntryIterator toVariantsCountIterator(SampleIndexEntry entry, String gt) {
        return toVariantsCountIterator(entry.getGtEntry(gt));
    }

    public SampleIndexEntryIterator toVariantsCountIterator(SampleIndexEntry.SampleIndexGtEntry gtEntry) {
        return new CountSampleIndexGtEntryIterator(gtEntry, schema);
    }

    public MendelianErrorSampleIndexEntryIterator toMendelianIterator(SampleIndexEntry sampleIndexEntry) {
        return new MendelianErrorSampleIndexEntryIterator(sampleIndexEntry, schema);
    }

    public SampleIndexSchema getSchema() {
        return schema;
    }

    private abstract static class SampleIndexGtEntryIterator implements SampleIndexEntryIterator {
        protected SampleIndexEntry.SampleIndexGtEntry gtEntry;
        private final SampleIndexSchema schema;
        private BitInputStream popFreq;
        private BitInputStream ctIndex;
        private BitInputStream btIndex;
        private BitInputStream tfIndex;
        private BitInputStream ctBtTfIndex;
        private int nonIntergenicCount;
        private int clinicalCount;
        private BitInputStream fileIndex;
        private ByteBuffer fileDataIndex;
        private int fileIndexCount; // Number of fileIndex elements visited
        private int fileIndexIdx;   // Index over file index array. Index of last visited fileIndex

        // Reuse the annotation index entry. Avoid create a new instance for each variant.
        private final AnnotationIndexEntry annotationIndexEntry;
        private int annotationIndexEntryIdx;
        private BitInputStream clinicalIndex;

        SampleIndexGtEntryIterator(SampleIndexSchema schema) {
            nonIntergenicCount = 0;
            clinicalCount = 0;
            annotationIndexEntry = AnnotationIndexEntry.empty(schema);
            annotationIndexEntryIdx = -1;
            fileIndexIdx = 0;
            fileIndexCount = 0;
            this.schema = schema;
        }

        SampleIndexGtEntryIterator(SampleIndexEntry.SampleIndexGtEntry gtEntry, SampleIndexSchema schema) {
            this(schema);
            this.gtEntry = gtEntry;
            this.ctIndex = gtEntry.getConsequenceTypeIndexStream();
            this.btIndex = gtEntry.getBiotypeIndexStream();
            this.tfIndex = gtEntry.getTranscriptFlagIndexStream();
            this.ctBtTfIndex = gtEntry.getCtBtTfIndexStream();
            this.popFreq = gtEntry.getPopulationFrequencyIndexStream();
            this.clinicalIndex = gtEntry.getClinicalIndexStream();
            this.fileIndex = gtEntry.getFileIndexStream();
            this.fileDataIndex = gtEntry.getFileDataIndexBuffer();
        }

        @Override
        public String nextGenotype() {
            return gtEntry.getGt();
        }

        @Override
        public boolean hasFileIndex() {
            return gtEntry.getFileIndex() != null;
        }

        @Override
        public boolean hasFileDataIndex() {
            return gtEntry.getFileData() != null;
        }

        @Override
        public boolean isMultiFileIndex() {
            return isMultiFileIndex(nextFileIndex());
        }

        public boolean isMultiFileIndex(int i) {
//            configuration.getFileIndex().getMultiFileIndex().readAndDecode()
            return schema.getFileIndex().isMultiFile(fileIndex, i);
        }

        private int nextFileIndex() {
            while (nextIndex() != fileIndexCount) {
                // Move index
                fileIndexIdx++;
                if (!isMultiFileIndex(fileIndexIdx - 1)) {
                    // If the previous fileIndex was not multifile, move counter
                    fileIndexCount++;
                }
            }
            return fileIndexIdx;
        }

        @Override
        public BitBuffer nextFileIndexEntry() {
            return getFileIndex(nextFileIndex());
        }

        @Override
        public ByteBuffer getFileDataEntry() {
            return getFileDataIndex(fileIndexIdx);
        }

        @Override
        public BitBuffer nextMultiFileIndexEntry() {
            if (isMultiFileIndex()) {
                fileIndexIdx++;
                return nextFileIndexEntry();
            } else {
                throw new NoSuchElementException();
            }
        }

        private BitBuffer getFileIndex(int i) {
            return schema.getFileIndex().readEntry(fileIndex, i);
        }

        private ByteBuffer getFileDataIndex(int i) {
            return schema.getFileData().readEntry(fileDataIndex, i);
        }

        @Override
        public boolean hasParentsIndex() {
            return gtEntry.getParentsIndex() != null;
        }

        @Override
        public byte nextParentsIndexEntry() {
            return gtEntry.getParentsIndex(nextIndex());
        }

        public AnnotationIndexEntry nextAnnotationIndexEntry() {
            if (annotationIndexEntryIdx == nextIndex()) {
                return annotationIndexEntry;
            }

            if (gtEntry.getAnnotationIndex() == null && popFreq == null) {
                return null;
            }

            int idx = nextIndex();
            annotationIndexEntry.clear();

            if (gtEntry.getAnnotationIndex() != null) {
                annotationIndexEntry.setSummaryIndex(gtEntry.getAnnotationIndex(idx));
                boolean nonIntergenic = AbstractSampleIndexEntryFilter.isNonIntergenic(annotationIndexEntry.getSummaryIndex());
                annotationIndexEntry.setIntergenic(!nonIntergenic);

                if (nonIntergenic) {
                    int nextNonIntergenic = nextNonIntergenicIndex();
                    if (ctIndex != null) {
                        annotationIndexEntry.setCtIndex(schema.getCtIndex().readFieldValue(ctIndex, nextNonIntergenic));
                    }
                    if (btIndex != null) {
                        annotationIndexEntry.setBtIndex(schema.getBiotypeIndex().readFieldValue(btIndex, nextNonIntergenic));
                    }
                    if (tfIndex != null) {
                        annotationIndexEntry.setTfIndex(schema.getTranscriptFlagIndexSchema().readFieldValue(tfIndex, nextNonIntergenic));
                    }

                    if (ctBtTfIndex != null
                            && annotationIndexEntry.getCtIndex() != 0
                            && annotationIndexEntry.getBtIndex() != 0
                            && annotationIndexEntry.getTfIndex() != 0) {
                        schema.getCtBtTfIndex().getField().read(
                                ctBtTfIndex,
                                annotationIndexEntry.getCtIndex(),
                                annotationIndexEntry.getBtIndex(),
                                annotationIndexEntry.getTfIndex(),
                                annotationIndexEntry.getCtBtTfCombination());
                    }
                }
            }

            if (popFreq != null) {
                // TODO: Reuse BitBuffer
                BitBuffer popFreqIndex = popFreq.readBitBuffer(schema.getPopFreqIndex().getBitsLength());
                annotationIndexEntry.setPopFreqIndex(popFreqIndex);
            }

            if (gtEntry.getClinicalIndex() != null) {
                boolean clinical = AbstractSampleIndexEntryFilter.isClinical(annotationIndexEntry.getSummaryIndex());
                annotationIndexEntry.setHasClinical(clinical);
                if (clinical) {
                    int nextClinical = nextClinicalIndex();
                    // TODO: Reuse BitBuffer
                    annotationIndexEntry.setClinicalIndex(schema.getClinicalIndexSchema().readEntry(clinicalIndex, nextClinical));
                }
            }

            annotationIndexEntryIdx = idx;
            return annotationIndexEntry;
        }

        @Override
        public int nextNonIntergenicIndex() {
            if (gtEntry.getAnnotationIndex() == null) {
                return -1;
            } else if (AbstractSampleIndexEntryFilter.isNonIntergenic(gtEntry.getAnnotationIndex(nextIndex()))) {
                return nonIntergenicCount;
            } else {
                throw new IllegalStateException("Next variant is not intergenic!");
            }
        }

        @Override
        public int nextClinicalIndex() {
            if (gtEntry.getAnnotationIndex() == null) {
                return -1;
            } else if (AbstractSampleIndexEntryFilter.isClinical(gtEntry.getAnnotationIndex(nextIndex()))) {
                return clinicalCount;
            } else {
                throw new IllegalStateException("Next variant is not clinical!");
            }
        }

        protected void increaseCounters() {
            // If the variant to be returned is non-intergenic, increase the number of non-intergenic variants.
            if (gtEntry.getAnnotationIndex() != null) {
                if (AbstractSampleIndexEntryFilter.isNonIntergenic(gtEntry.getAnnotationIndex(nextIndex()))) {
                    nonIntergenicCount++;
                }
            }
            // If the variant to be returned is clinical, increase the number of clinical variants.
            if (gtEntry.getAnnotationIndex() != null) {
                if (AbstractSampleIndexEntryFilter.isClinical(gtEntry.getAnnotationIndex(nextIndex()))) {
                    clinicalCount++;
                }
            }
        }

    }

    private static final class EmptySampleIndexEntryIterator implements SampleIndexEntryIterator {

        private EmptySampleIndexEntryIterator() {
        }

        private static final EmptySampleIndexEntryIterator EMPTY_ITERATOR = new EmptySampleIndexEntryIterator();

        public static SampleIndexEntryIterator emptyIterator() {
            return EmptySampleIndexEntryIterator.EMPTY_ITERATOR;
        }

        @Override
        public int nextIndex() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public String nextGenotype() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public int nextNonIntergenicIndex() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public int nextClinicalIndex() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public void skip() {
            throw new NoSuchElementException("Empty iterator");
        }

        public int getApproxSize() {
            return 0;
        }

        @Override
        public boolean hasFileIndex() {
            return false;
        }

        @Override
        public boolean hasFileDataIndex() {
            return false;
        }

        @Override
        public BitBuffer nextFileIndexEntry() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public ByteBuffer getFileDataEntry() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public boolean isMultiFileIndex() {
            return false;
        }

        @Override
        public BitBuffer nextMultiFileIndexEntry() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public boolean hasParentsIndex() {
            return false;
        }

        @Override
        public byte nextParentsIndexEntry() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public AnnotationIndexEntry nextAnnotationIndexEntry() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public Variant next() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public Variant nextVariant() {
            throw new NoSuchElementException("Empty iterator");
        }
    }

    private static final class CountSampleIndexGtEntryIterator extends SampleIndexGtEntryIterator {

        private final int count;
        private int i;
        private static final Variant DUMMY_VARIANT = new Variant("1:10:A:T");

        CountSampleIndexGtEntryIterator(SampleIndexEntry.SampleIndexGtEntry gtEntry, SampleIndexSchema configuration) {
            super(gtEntry, configuration);
            count = gtEntry.getCount();
            i = 0;
        }

        @Override
        public int nextIndex() {
            return i;
        }

        @Override
        public boolean hasNext() {
            return i != count;
        }

        @Override
        public void skip() {
            nextAnnotationIndexEntry(); // ensure read annotation
            increaseCounters();
            i++;
        }

        @Override
        public Variant next() {
            skip();
            return DUMMY_VARIANT;
        }

        @Override
        public Variant nextVariant() {
            return DUMMY_VARIANT;
        }

        @Override
        public int getApproxSize() {
            return count;
        }
    }

    private class ByteSampleIndexGtEntryIterator extends SampleIndexGtEntryIterator {
        private final String chromosome;
        private final int batchStart;
        private final byte[] bytes;
        private final int length;
        private final int offset;
        private int currentOffset;

        private int idx;

        private boolean hasNext;
        private boolean encodedRefAlt;
        private int variantLength;
        private int referenceLength;
        private int alternateLength;

        ByteSampleIndexGtEntryIterator(String chromosome, int batchStart, SampleIndexEntry.SampleIndexGtEntry gtEntry,
                                       SampleIndexSchema schema) {
            super(gtEntry, schema);
            this.chromosome = chromosome;
            this.batchStart = batchStart;
            this.bytes = gtEntry.getVariants();
            this.offset = gtEntry.getVariantsOffset();
            this.length = gtEntry.getVariantsLength();
            this.currentOffset = offset;

            this.idx = -1;
            movePointer();
        }

        @Override
        public int nextIndex() {
            return idx;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Variant next() {
            nextAnnotationIndexEntry(); // ensure read annotation
            increaseCounters();
            Variant variant = nextVariant();
            movePointer();
            return variant;
        }

        @Override
        public Variant nextVariant() {
            Variant variant;
            if (encodedRefAlt) {
                variant = toVariantEncodedAlleles(chromosome, batchStart, bytes, currentOffset);
            } else {
                variant = toVariant(chromosome, batchStart, bytes, currentOffset, referenceLength, alternateLength);
            }
            return variant;
        }

        @Override
        public void skip() {
            nextAnnotationIndexEntry(); // ensure read annotation
            increaseCounters();
            movePointer();
        }

        @Override
        public int getApproxSize() {
            if (gtEntry.getCount() > 0) {
                return gtEntry.getCount();
            }
            double expectedVariantSize = 8.0;
            double approximation = 1.2;
            return (int) (length / expectedVariantSize * approximation);
        }

        private void movePointer() {
            currentOffset += variantLength;

            if (length - (currentOffset - offset) >= (INT24_LENGTH)) {
                hasNext = true;
                idx++;
                if (hasEncodedAlleles(bytes, currentOffset)) {
                    encodedRefAlt = true;
                    variantLength = INT24_LENGTH;
                } else {
                    encodedRefAlt = false;
                    referenceLength = readNextSeparator(bytes, currentOffset + INT24_LENGTH);
                    alternateLength = readNextSeparator(bytes, currentOffset + INT24_LENGTH + referenceLength + SEPARATOR_LENGTH);
                    variantLength = INT24_LENGTH + referenceLength + SEPARATOR_LENGTH + alternateLength + SEPARATOR_LENGTH;
                }
            } else {
                hasNext = false;
                referenceLength = 0;
                alternateLength = 0;
                variantLength = 0;
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ByteSampleIndexGtEntryIterator{");
            sb.append("chromosome='").append(chromosome).append('\'');
            sb.append(", batchStart=").append(batchStart);
            sb.append(", gt='").append(gtEntry.getGt()).append('\'');
            sb.append(", SampleIndexGtEntry='").append(gtEntry.toString()).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    public static boolean hasEncodedAlleles(byte[] bytes, int offset) {
        return (bytes[offset] & 0xF0) != 0;
    }

    private Variant toVariantEncodedAlleles(String chromosome, int batchStart, byte[] bytes, int offset) {
        String[] refAlt = AlleleSnvCodec.decode(bytes[offset]);
        int start = batchStart + (read24bitInteger(bytes, offset) & 0x0F_FF_FF);

        return VariantPhoenixKeyFactory.buildVariant(chromosome, start, refAlt[0], refAlt[1], null, null);
    }

    private Variant toVariant(String chromosome, int batchStart, byte[] bytes, int offset, int referenceLength, int alternateLength) {
        int start = batchStart + read24bitInteger(bytes, offset);
        offset += INT24_LENGTH;
        String reference = readString(bytes, offset, referenceLength);
        offset += referenceLength + SEPARATOR_LENGTH; // add reference, and separator
        String alternate = readString(bytes, offset, alternateLength);

        return VariantPhoenixKeyFactory.buildVariant(chromosome, start, reference, alternate, null, null);
    }

    private int readNextSeparator(byte[] bytes, int offset) {
        for (int i = offset; i < bytes.length; i++) {
            if (bytes[i] == 0) {
                return i - offset;
            }
        }
        return bytes.length - offset;
    }

    protected int getRelativeStart(Variant variant) {
        return variant.getStart() % SampleIndexSchema.BATCH_SIZE;
    }

    protected String getAlternate(Variant v) {
        return VariantPhoenixKeyFactory.buildSymbolicAlternate(v.getReference(), v.getAlternate(), v.getEnd(), v.getSv());
    }

    /**
     * Append a 24bit Integer.
     *
     * @param n      the integer to serialize.
     * @param bytes  the byte array into which to put the serialized form of object
     * @param offset the offset from which to start writing the serialized form
     * @return the byte length of the serialized object
     */
    protected int append24bitInteger(int n, byte[] bytes, int offset) {
        bytes[offset + 2] = (byte) n;
        n >>>= 8;
        bytes[offset + 1] = (byte) n;
        n >>>= 8;
        bytes[offset] = (byte) n;

        return INT24_LENGTH;
    }

    protected int read24bitInteger(byte[] bytes, int offset) {
        int n = bytes[offset] & 255;
        n <<= 8;
        n ^= bytes[offset + 1] & 255;
        n <<= 8;
        n ^= bytes[offset + 2] & 255;

        return n;
    }

    /**
     * Serialize string.
     *
     * @param str    The string to serialize
     * @param bytes  the byte array into which to put the serialized form of object
     * @param offset the offset from which to start writing the serialized form
     * @return the byte length of the serialized object
     */
    protected int appendString(String str, byte[] bytes, int offset) {
        return PVarchar.INSTANCE.toBytes(str, bytes, offset);
    }

    protected String readString(byte[] bytes, int offset, int length) {
        return (String) PVarchar.INSTANCE.toObject(bytes, offset, length);
    }

    /**
     * Append a separator.
     *
     * @param bytes  the byte array into which to put the serialized form of object
     * @param offset the offset from which to start writing the serialized form
     * @return the byte length of the serialized object
     */
    protected int appendSeparator(byte[] bytes, int offset) {
        bytes[offset] = BYTE_SEPARATOR;
        return SEPARATOR_LENGTH;
    }

    public static List<String> split(byte[] value) {
        return split(value, 0, value.length);
    }

    public static List<String> split(byte[] value, int offset, int length) {
        List<String> values = new ArrayList<>(length / 10);
        int valueOffset = offset;
        for (int i = offset; i < length + offset; i++) {
            if (value[i] == STRING_SEPARATOR) {
                if (i != valueOffset) { // Skip empty values
                    values.add(Bytes.toString(value, valueOffset, i - valueOffset));
                }
                valueOffset = i + 1;
            }
        }
        if (length + offset != valueOffset) { // Skip empty values
            values.add(Bytes.toString(value, valueOffset, length + offset - valueOffset));
        }
        return values;
    }
}
