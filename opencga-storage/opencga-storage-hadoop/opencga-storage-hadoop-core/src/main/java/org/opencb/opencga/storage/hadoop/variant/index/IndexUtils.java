package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.RangeIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;

/**
 * Created on 01/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class IndexUtils {

    public static final byte EMPTY_MASK = 0;

    private IndexUtils() {
    }

    public static String byteToString(byte b) {
        return binaryToString(b, Byte.SIZE);
    }

    public static String bytesToString(byte[] bytes) {
        StringBuilder sb = new StringBuilder("[ ");
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            sb.append(byteToString(b));
            sb.append(" , ");
        }
        return sb.append(" ]").toString();
    }

    public static String shortToString(short s) {
        return binaryToString(s, Short.SIZE);
    }

    public static String binaryToString(int number, int bits) {
        String str = Integer.toBinaryString(number);
        if (str.length() > bits) {
            str = str.substring(str.length() - bits);
        }
        return StringUtils.leftPad(str, bits, '0');
    }

    public static String maskToString(byte[] maskAndIndex) {
        return maskToString(maskAndIndex[0], maskAndIndex[1]);
    }

    /**
     * Merge the mask and the index into one single string.
     *
     * Mask   : 00101110
     * Index  : 00100100
     * Result : **1*010*
     *
     * @param mask      Mask
     * @param index     Index
     * @return      String representation
     */
    public static String maskToString(byte mask, byte index) {
        String maskStr = byteToString(mask);
        String indexStr = byteToString(index);

        StringBuilder sb = new StringBuilder(8);
        for (int i = 0; i < 8; i++) {
            if (maskStr.charAt(i) == '1') {
                sb.append(indexStr.charAt(i));
            } else {
                sb.append('*');
            }
        }
        return sb.toString();
    }

    public static boolean testParentsGenotypeCode(byte parentsCode, boolean[] fatherFilter, boolean[] motherFilter) {
        byte[] split = GenotypeCodec.split(parentsCode);
        return fatherFilter[split[0]] && motherFilter[split[1]];
    }

    public static String parentFilterToString(boolean[] filter) {
        StringBuilder sb = new StringBuilder();
        for (boolean b : filter) {
            sb.append(b ? "1" : "0");
        }
        return sb.toString();
    }

    public static boolean testIndex(byte indexValue, byte indexMask, byte indexFilter) {
        return (indexValue & indexMask) == indexFilter;
    }

    public static boolean testIndexAny(byte indexValue, byte indexMask) {
        return indexMask == 0 || (indexValue & indexMask) != 0;
    }

    public static boolean testIndexAny(short indexValue, short indexMask) {
        return indexMask == 0 || (indexValue & indexMask) != 0;
    }

    public static boolean testIndexAny(int indexValue, int indexMask) {
        return indexMask == 0 || (indexValue & indexMask) != 0;
    }

    public static boolean testIndexAny(byte[] index, int indexPosition, short indexMask) {
        return (Bytes.toShort(index, indexPosition * Short.BYTES, Short.BYTES) & indexMask) != 0;
    }

    /**
     * Check if an index can be used given an Operator and a Value.
     *
     * <code>
     *     -----------===========    --> Index covering
     *     0000000000011111111111    --> Index values
     *
     *     Operator "="
     *     -----------xxxxxQxxxxx    --> Positive index covers partially
     *     xxxxxQxxxxx-----------    --> Negated index covers partially
     *
     *     Operator "<"
     *     QQQQQQxxxxx-----------    --> Negated index covers partially.
     *     QQQQQQQQQQQ######-----    --> Can not use index!
     *
     *     Operator ">"
     *     -----------xxxQQQQQQQQ    --> Positive index covers partially.
     *     -----######QQQQQQQQQQQ    --> Can not use index!
     *
     *     = : Values where the index filter is true
     *     Q : Requested values
     *     x : Extra returned values
     *     # : Requested but missing values. Invalid index if any
     * </code>
     *
     *
     * @param op                Query operator
     * @param value             Query value
     * @param indexThreshold    Index threshold
     * @return                  Null if the index can not be applied. True or False to indicate how to use the index.
     */
    public static Boolean intersectIndexGreaterThan(String op, double value, double indexThreshold) {
        switch (op) {
            case "":
            case "=":
            case "==":
                if (value > indexThreshold) {
                    return Boolean.TRUE;
                } else {
                    return Boolean.FALSE;
                }
            case "<=":
            case "<<=":
                // No need to add DELTA to value, as the index negated is already covering the "<=" operator
            case "<":
            case "<<":
                if (value <= indexThreshold) {
                    return Boolean.FALSE;
                } else {
                    return null;
                }
            case ">=":
            case ">>=":
                // Index is using operator ">". To use ">=" operator, need to subtract a DELTA to the value
                value -= RangeIndexFieldFilter.DELTA;
            case ">":
            case ">>":
                if (value >= indexThreshold) {
                    return Boolean.TRUE;
                } else {
                    return null;
                }
            default:
                throw new VariantQueryException("Unknown query operator" + op);
        }
    }

    public static byte[] countPerBitToBytes(int[] counts) {
        byte[] bytes = new byte[8 * Bytes.SIZEOF_INT];
        int offset = 0;
        for (int count : counts) {
            offset += PUnsignedInt.INSTANCE.getCodec().encodeInt(count, bytes, offset);
        }
        return bytes;
    }

    public static int[] countPerBitToObject(byte[] bytes) {
        return countPerBitToObject(bytes, 0, bytes.length);
    }

    public static int[] countPerBitToObject(byte[] bytes, int offset, int length) {
        int[] counts = new int[8];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = Bytes.toInt(bytes, offset);
            offset += Bytes.SIZEOF_INT;
        }
        return counts;
    }

    public static int[] countPerBit(byte[] bytes) {
        int[] counts = new int[8];
        for (byte b : bytes) {
            counts[0] += (b >>> 0) & 1;
            counts[1] += (b >>> 1) & 1;
            counts[2] += (b >>> 2) & 1;
            counts[3] += (b >>> 3) & 1;
            counts[4] += (b >>> 4) & 1;
            counts[5] += (b >>> 5) & 1;
            counts[6] += (b >>> 6) & 1;
            counts[7] += (b >>> 7) & 1;
        }
        return counts;
    }

    public static int getByte1(int v) {
        return v & 0xFF;
    }

    public static int getByte2(int v) {
        return getByte1(v >> Byte.SIZE);
    }

    public static int log2(int i) {
        return 31 - Integer.numberOfLeadingZeros(i);
    }
}
