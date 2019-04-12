package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.Variant;

import java.util.Comparator;

import static org.apache.hadoop.hbase.util.Bytes.SIZEOF_INT;

/**
 * Define RowKey and column names.
 *
 * Created on 11/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class SampleIndexSchema {

    public static final int BATCH_SIZE = 1_000_000;
    public static final Comparator<Variant> INTRA_CHROMOSOME_VARIANT_COMPARATOR = Comparator.comparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(Variant::toString);
    static final byte[] MENDELIAN_ERROR_COLUMN = Bytes.toBytes("ME");
    static final char META_PREFIX = '_';
    static final String PARENTS_PREFIX = META_PREFIX + "P_";
    static final byte[] PARENTS_PREFIX_BYTES = Bytes.toBytes(PARENTS_PREFIX);
    static final String FILE_PREFIX = META_PREFIX + "F_";
    static final byte[] FILE_PREFIX_BYTES = Bytes.toBytes(FILE_PREFIX);
    static final String ANNOTATION_PREFIX = META_PREFIX + "A_";
    static final byte[] ANNOTATION_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_PREFIX);
    static final String GENOTYPE_COUNT_PREFIX = META_PREFIX + "C_";
    static final String PENDING_VARIANT_PREFIX = META_PREFIX + "V_";
    static final byte[] PENDING_VARIANT_PREFIX_BYTES = Bytes.toBytes(PENDING_VARIANT_PREFIX);

    private SampleIndexSchema() {
    }

    public static int getExpectedSize(String chromosome) {
        int expectedSize;
        if (chromosome == null) {
            expectedSize = SIZEOF_INT;
        } else {
            expectedSize = SIZEOF_INT + chromosome.length() + 1 + SIZEOF_INT;
        }
        return expectedSize;
    }

    public static byte[] toRowKey(int sample) {
        return toRowKey(sample, null, 0);
    }

    public static byte[] toRowKey(int sample, String chromosome, int position) {
        int expectedSize = getExpectedSize(chromosome);
        byte[] rk = new byte[expectedSize];

        toRowKey(sample, chromosome, position, rk);

        return rk;
    }

    private static int toRowKey(int sample, String chromosome, int position, byte[] rk) {
        int offset = 0;
        offset += PInteger.INSTANCE.toBytes(sample, rk, offset);


        if (chromosome != null) {
            offset += PVarchar.INSTANCE.toBytes(chromosome, rk, offset);
            rk[offset] = 0;
            offset++;
            offset += PInteger.INSTANCE.toBytes(position / BATCH_SIZE, rk, offset);
        }
        return offset;
    }

    public static String rowKeyToString(byte[] row) {
        if (row == null || row.length == 0) {
            return null;
        }
        Object sampleId = PInteger.INSTANCE.toObject(row, 0, 4);
        if (row.length > 5) {
            Object chr = chromosomeFromRowKey(row);
            try {
                Object pos = batchStartFromRowKey(row);
                return sampleId + "_" + chr + "_" + pos;
            } catch (RuntimeException e) {
                return sampleId + "_" + chr + "_########";
            }
        } else {
            return sampleId + "_";
        }
    }

    public static String chromosomeFromRowKey(byte[] row) {
        return (String) PVarchar.INSTANCE.toObject(row, 4, row.length - 4 - 1 - 4);
    }

    public static int batchStartFromRowKey(byte[] row) {
        return ((Integer) PInteger.INSTANCE.toObject(row, row.length - 4, 4)) * BATCH_SIZE;
    }

    public static byte[] toGenotypeColumn(String genotype) {
        return Bytes.toBytes(genotype);
    }

    public static byte[] toGenotypeCountColumn(String genotype) {
        return Bytes.toBytes(GENOTYPE_COUNT_PREFIX + genotype);
    }

    public static byte[] toPendingColumn(Variant variant, String gt) {
        return Bytes.toBytes(PENDING_VARIANT_PREFIX + variant.toString() + '_' + gt);
    }

    public static byte[] toAnnotationIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_PREFIX + genotype);
    }

    public static byte[] toFileIndexColumn(String genotype) {
        return Bytes.toBytes(FILE_PREFIX + genotype);
    }

    public static String getGt(Cell cell, byte[] prefix) {
        return Bytes.toString(
                cell.getQualifierArray(),
                cell.getQualifierOffset() + prefix.length,
                cell.getQualifierLength() - prefix.length);
    }

    public static byte[] toMendelianErrorColumn() {
        return MENDELIAN_ERROR_COLUMN;
    }

    public static byte[] toParentsGTColumn(String genotype) {
        return Bytes.toBytes(PARENTS_PREFIX + genotype);
    }
}
