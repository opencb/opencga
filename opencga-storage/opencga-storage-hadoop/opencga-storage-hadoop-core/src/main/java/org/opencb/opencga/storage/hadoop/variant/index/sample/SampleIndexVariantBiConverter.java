package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.MENDELIAN_ERROR_COLUMN;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.META_PREFIX;

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

    public int expectedSize(Variant variant) {
        return INT24_LENGTH + variant.getReference().length() + SEPARATOR_LENGTH + getAlternate(variant).length();
    }

    protected int expectedSize(String reference, String alternate) {
        return INT24_LENGTH + reference.length() + 1 + alternate.length();
    }

    @Deprecated
    public byte[] toBytesSimpleString(Collection<Variant> variants) {
        return Bytes.toBytes(variants.stream().map(Variant::toString).collect(Collectors.joining(",")));
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
            size += expectedSize(variant);
            size += SEPARATOR_LENGTH;
        }
        byte[] bytes = new byte[size];
        toBytes(variants, bytes, 0);
        return bytes;
    }

    protected int toBytes(Collection<Variant> variants, byte[] bytes, int offset) {
        int length = 0;
        for (Variant variant : variants) {
            length += toBytes(variant, bytes, offset + length);
            length += appendSeparator(bytes, offset + length);
        }
        return length;
    }

    public byte[] toBytes(Variant variant) {
        String alternate = getAlternate(variant);
        byte[] bytes = new byte[expectedSize(variant.getReference(), alternate)];
        toBytes(getRelativeStart(variant), variant.getReference(), alternate, bytes, 0);
        return bytes;
    }

    public void toBytes(Variant variant, ByteArrayOutputStream stream) throws IOException {
        if (stream.size() != 0) {
            // separator
            stream.write(BYTE_SEPARATOR);
        }
        stream.write(toBytes(variant));
    }

    public int toBytes(Variant variant, byte[] bytes, int offset) {
        return toBytes(getRelativeStart(variant), variant.getReference(), getAlternate(variant), bytes, offset);
    }

    protected int toBytes(int relativeStart, String reference, String alternate, byte[] bytes, int offset) {
        int length = 0;
        length += append24bitInteger(relativeStart, bytes, offset + length);
        length += appendString(reference, bytes, offset + length);
        length += appendSeparator(bytes, offset + length);
        length += appendString(alternate, bytes, offset + length);
        return length;
    }

    public Variant toVariant(String chromosome, int batchStart, byte[] bytes) {
        return toVariant(chromosome, batchStart, bytes, 0);
    }

    public Variant toVariant(String chromosome, int batchStart, byte[] bytes, int offset) {
        int referenceLength = readNextSeparator(bytes, offset + INT24_LENGTH);
        int alternateLength = readNextSeparator(bytes, offset + INT24_LENGTH + referenceLength + SEPARATOR_LENGTH);
        return toVariant(chromosome, batchStart, bytes, offset, referenceLength, alternateLength);
    }

    public List<Variant> toVariants(Cell cell) {
        List<Variant> variants;
        byte[] column = CellUtil.cloneQualifier(cell);
        if (column[0] != META_PREFIX && column[0] != MENDELIAN_ERROR_COLUMN[0]) {
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
        LinkedList<Variant> variants = new LinkedList<>();
        toVariantsIterator(chromosome, batchStart, bytes, offset, length).forEachRemaining(variants::add);
        return variants;
    }

    public SampleIndexVariantIterator toVariantsIterator(String chromosome, int batchStart, byte[] bytes, int offset, int length) {
        if (length <= 0) {
            return EmptySampleIndexVariantIterator.emptyIterator();
        } else {
            // Compare only the first letters to run a "startsWith"
            byte[] startsWith = Bytes.toBytes(chromosome + ':');
            int compareLength = startsWith.length;
            if (length > compareLength
                    && Bytes.compareTo(bytes, offset, compareLength, startsWith, 0, compareLength) == 0) {
                return new StringSampleIndexVariantIterator(bytes, offset, length);
            } else {
                return new ByteSampleIndexVariantIterator(chromosome, batchStart, bytes, offset, length);
            }
        }
    }

    public interface SampleIndexVariantIterator extends Iterator<Variant> {
        /**
         * @return the index of the element that would be returned by a
         * subsequent call to {@code next}.
         */
        int nextIndex();

        /**
         * @return {@code true} if the iteration has more elements
         */
        boolean hasNext();

        /**
         * Skip next element. Avoid conversion.
         */
        void skip();

        /**
         * @return next variant
         */
        Variant next();
    }

    private static final class EmptySampleIndexVariantIterator implements SampleIndexVariantIterator {

        private EmptySampleIndexVariantIterator() {
        }

        private static final EmptySampleIndexVariantIterator EMPTY_ITERATOR = new EmptySampleIndexVariantIterator();

        public static SampleIndexVariantIterator emptyIterator() {
            return EMPTY_ITERATOR;
        }

        @Override
        public int nextIndex() {
            return 0;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public void skip() {
        }

        @Override
        public Variant next() {
            throw new NoSuchElementException("Empty iterator");
        }
    }

    private class StringSampleIndexVariantIterator implements SampleIndexVariantIterator {
        private final ListIterator<String> variants;

        StringSampleIndexVariantIterator(byte[] value, int offset, int length) {
            variants = split(value, offset, length).listIterator();
        }

        @Override
        public int nextIndex() {
            return variants.nextIndex();
        }

        @Override
        public boolean hasNext() {
            return variants.hasNext();
        }

        @Override
        public void skip() {
            variants.next();
        }

        @Override
        public Variant next() {
            return new Variant(variants.next());
        }
    }

    private class ByteSampleIndexVariantIterator implements SampleIndexVariantIterator {
        private final String chromosome;
        private final int batchStart;
        private final byte[] bytes;
        private final int length;
        private final int offset;
        private int currentOffset;

        private int idx;

        private boolean hasNext;
        private int variantLength;
        private int referenceLength;
        private int alternateLength;

        ByteSampleIndexVariantIterator(String chromosome, int batchStart, byte[] bytes, int offset, int length) {
            this.chromosome = chromosome;
            this.batchStart = batchStart;
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
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
            Variant variant = toVariant(chromosome, batchStart, bytes, currentOffset, referenceLength, alternateLength);
            movePointer();
            return variant;
        }

        @Override
        public void skip() {
            movePointer();
        }

        private void movePointer() {
            currentOffset += variantLength;

            if (length - (currentOffset - offset) > (INT24_LENGTH + SEPARATOR_LENGTH)) {
                hasNext = true;
                idx++;
                referenceLength = readNextSeparator(bytes, currentOffset + INT24_LENGTH);
                alternateLength = readNextSeparator(bytes, currentOffset + INT24_LENGTH + referenceLength + SEPARATOR_LENGTH);
                variantLength = INT24_LENGTH + referenceLength + SEPARATOR_LENGTH + alternateLength + SEPARATOR_LENGTH;
            } else {
                hasNext = false;
                referenceLength = 0;
                alternateLength = 0;
                variantLength = 0;
            }
        }
    }

    private Variant toVariant(String chromosome, int batchStart, byte[] bytes, int offset, int referenceLength, int alternateLength) {
        int start = batchStart + read24bitInteger(bytes, offset);
        offset += INT24_LENGTH;
        String reference = readString(bytes, offset, referenceLength);
        offset += referenceLength + SEPARATOR_LENGTH; // add reference, and separator
        String alternate = readString(bytes, offset, alternateLength);

        return VariantPhoenixKeyFactory.buildVariant(chromosome, start, reference, alternate, null);
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
