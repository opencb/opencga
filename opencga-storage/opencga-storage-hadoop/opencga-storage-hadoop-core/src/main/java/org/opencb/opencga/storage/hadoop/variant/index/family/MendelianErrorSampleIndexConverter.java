package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntryFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexVariantBiConverter.SampleIndexVariantIterator;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexVariantBiConverter.split;

/**
 * Created on 11/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MendelianErrorSampleIndexConverter {

    protected static final byte SEPARATOR = ',';
    protected static final byte MENDELIAN_ERROR_SEPARATOR = '_';
    protected static final byte MENDELIAN_ERROR_CODE_SEPARATOR = ':'; // optional

    public static void toBytes(ByteArrayOutputStream stream, Variant variant, String gt, int gtIdx, int errorCode) throws IOException {
        if (stream.size() != 0) {
            stream.write(SEPARATOR);
        }
        stream.write(Bytes.toBytes(variant.toString()));
        stream.write(MENDELIAN_ERROR_SEPARATOR);
        stream.write(Bytes.toBytes(gt));
        stream.write(MENDELIAN_ERROR_SEPARATOR);
        stream.write(Bytes.toBytes(Integer.toString(gtIdx)));
        stream.write(MENDELIAN_ERROR_CODE_SEPARATOR);
        stream.write(Bytes.toBytes(Integer.toString(errorCode)));
    }

    public static MendelianErrorSampleIndexVariantIterator toVariants(byte[] bytes, int offset, int length) {
        return new MendelianErrorSampleIndexVariantIterator(bytes, offset, length);
    }

    public static class MendelianErrorSampleIndexVariantIterator extends SampleIndexVariantIterator {
        private final ListIterator<String> variants;
        private final int size;
        private Variant next;
        private int nextIndex;
        private int nextCode;
        private String nextGt;
        private Map<String, NonIntergenicCount> gtIntergenicCount = Collections.emptyMap();

        private static class NonIntergenicCount {
            // Annotation index itself
            private byte[] annotationIndex;
            private int nonIntergenicCount = 0;
            private int lastNonIntergenicVariantIdx = -1;

            NonIntergenicCount(byte[] annotationIndex) {
                this.annotationIndex = annotationIndex;
            }
        }

        public MendelianErrorSampleIndexVariantIterator(byte[] value, int offset, int length) {
            List<String> values = split(value, offset, length);
            size = values.size();
            variants = values.listIterator();
        }

        @Override
        public int nextIndex() {
            if (next == null) {
                fetchNext();
            }
            return nextIndex;
        }

        private NonIntergenicCount getNonIntergenicCount() {
            if (gtIntergenicCount == null) {
                return null;
            }
            return gtIntergenicCount.get(nextGenotype());
        }

        @Override
        public int nextNonIntergenicIndex() {
            NonIntergenicCount count = getNonIntergenicCount();
            if (count == null) {
                return -1;
            } else {
                int idx = nextIndex();

                if (count.lastNonIntergenicVariantIdx == idx) {
                    // nonIntergenicCount includes next variant
                    return count.nonIntergenicCount - 1;
                } else if (SampleIndexEntryFilter.isNonIntergenic(count.annotationIndex, idx)) {

                    // Loop from next of lastNonIntergenic variant to prev of nextVariant
                    // Do not check if the next variant is intergenic or not. Already checked.
                    for (int i = count.lastNonIntergenicVariantIdx + 1; i < nextIndex - 1; i++) {
                        if (SampleIndexEntryFilter.isNonIntergenic(count.annotationIndex, i)) {
                            count.nonIntergenicCount++;
                        }
                    }

                    // Next variant is intergenic. Checked in if condition
                    count.nonIntergenicCount++;

                    count.lastNonIntergenicVariantIdx = idx;
                    // nonIntergenicCount includes next variant
                    return count.nonIntergenicCount - 1;
                } else {
                    throw new IllegalStateException("Next variant is not intergenic!");
                }
            }
        }

        public String nextGenotype() {
            if (next == null) {
                fetchNext();
            }
            return nextGt;
        }

        public int nextCode() {
            if (next == null) {
                fetchNext();
            }
            return nextCode;
        }

        @Override
        public boolean hasNext() {
            return variants.hasNext();
        }

        @Override
        public void skip() {
            next();
        }

        @Override
        public Variant next() {
            if (next == null) {
                fetchNext();
            }
            if (next == null) {
                throw new NoSuchElementException();
            }
            Variant variant = next;
            next = null; // Clean next variant
            return variant;
        }

        @Override
        public int getApproxSize() {
            return size;
        }

        private void fetchNext() {
            if (variants.hasNext()) {
                String s = variants.next();
                int idx2 = s.lastIndexOf(MENDELIAN_ERROR_SEPARATOR);
                int idx1 = s.lastIndexOf(MENDELIAN_ERROR_SEPARATOR, idx2 - 1);
                String variantStr = s.substring(0, idx1);
                nextGt = s.substring(idx1 + 1, idx2);
                String idxCode = s.substring(idx2 + 1);
                int i = idxCode.lastIndexOf(MENDELIAN_ERROR_CODE_SEPARATOR);
                nextIndex = i == StringUtils.INDEX_NOT_FOUND
                        ? Integer.valueOf(idxCode)
                        : Integer.valueOf(idxCode.substring(0, i));
                nextCode = i == StringUtils.INDEX_NOT_FOUND
                        ? 0
                        : Integer.valueOf(idxCode.substring(i + 1));
                next = new Variant(variantStr);
            } else {
                next = null;
                nextGt = null;
                nextIndex = -1;
            }
        }

        public void addAnnotationIndex(String gt, byte[] annotationIndex) {
            if (gtIntergenicCount == Collections.EMPTY_MAP) {
                gtIntergenicCount = new HashMap<>();
            }
            gtIntergenicCount.put(gt, new NonIntergenicCount(annotationIndex));
        }
    }

}
